#!/bin/bash

# Exit on error, treat unset variables as an error, and propagate exit status through pipes
set -euo pipefail
# For debugging:
# set -x
echo "DEBUG: Script PATH is: $PATH"

# --- Configuration ---
# === EC2 Paths (ADJUST THESE TO YOUR EC2 SETUP) ===
BASE_WORK_DIR="/mnt/nvme/hf" # Main directory for all pipeline operations

# Source data and initial batching
GIT_REPO_URL="https://huggingface.co/datasets/allenai/dolma"
GIT_CLONE_DIR="${BASE_WORK_DIR}/source_dolma_dataset"      # Where allenai/dolma is cloned
RAW_URL_BATCHES_PARENT_DIR="${BASE_WORK_DIR}/url_batches" # Parent dir for subdirs like 'allenai_dolma_commoncrawl_batches'

# Per-batch processing directories
TEMP_WGET_DOWNLOAD_PARENT_DIR="${BASE_WORK_DIR}/temp_wget_downloads" # Parent for temporary content downloaded by wget
PROCESSED_PARQUET_PARENT_DIR="${BASE_WORK_DIR}/processed_parquet"   # Parent for local .parquet files before S3/HF upload
TEMP_MANIFEST_PARENT_DIR="${BASE_WORK_DIR}/temp_manifests"          # Directory to store temporary manifest CSV files

# State tracking
STATE_DIR="${BASE_WORK_DIR}/processing_state" # To mark completed batches

# === Hugging Face Configuration ===
HF_USERNAME="nhagar"                 # Your Hugging Face username
HF_BASE_DATASET_NAME="dolma" # Base name, e.g., "dolma_content" -> "nhagar/dolma_content_commoncrawl"
HF_VARIANT="v1.5"        # Optional variant tag for the HF dataset repo
DATASET_NAME_S3="${HF_BASE_DATASET_NAME}_urls_${HF_VARIANT}"

# === S3 Configuration ===
S3_BUCKET_BASE_PATH="s3://hf-datasets-nh/${DATASET_NAME_S3}" # Base S3 path

# === Tool Configuration ===
PYTHON_EXEC="~/.local/bin/uv run"
WGET_PARALLEL_JOBS=10       # Number of parallel wget downloads per batch
WGET_TIMEOUT=45             # Timeout for each wget request in seconds (increased for content)
WGET_TRIES=2                # Number of retries for wget
URL_BATCH_SIZE=300          # Number of URLs per batch file
USER_AGENT="Mozilla/5.0 (compatible; MyResearchBot/1.0; +http://example.com/botinfo)" # Polite User-Agent

# --- Helper Functions ---
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - INFO - $1"
}
warn() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - WARN - $1" >&2
}
error_exit() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - ERROR - $1" >&2
    exit 1
}

# --- Ensure Directories Exist ---
mkdir -p "${BASE_WORK_DIR}" "${GIT_CLONE_DIR}" "${RAW_URL_BATCHES_PARENT_DIR}" \
         "${TEMP_WGET_DOWNLOAD_PARENT_DIR}" "${PROCESSED_PARQUET_PARENT_DIR}" \
         "${STATE_DIR}" "${TEMP_MANIFEST_PARENT_DIR}"
log "All necessary base directories checked/created."

# --- Configure AWS S3 CLI ---
aws configure set default.s3.max_concurrent_requests 64
aws configure set default.s3.multipart_chunksize 64MB
log "AWS S3 CLI configured."

# --- Stage 1: Clone/Update Dolma URL dataset and Batch URL Lists ---
# (This stage is identical to the previous version of run_pipeline.sh)
log "=== STAGE 1: Fetching and Batching URL Lists ==="
if [ ! -d "${GIT_CLONE_DIR}/.git" ]; then
    log "Cloning Dolma dataset from ${GIT_REPO_URL} into ${GIT_CLONE_DIR}..."
    git clone "${GIT_REPO_URL}" "${GIT_CLONE_DIR}"
else
    log "Updating existing Dolma dataset in ${GIT_CLONE_DIR}..."
    (cd "${GIT_CLONE_DIR}" && git pull --ff-only) # Use --ff-only or handle merge conflicts appropriately
fi
log "Removing any existing sample files (e.g., *-sample*.txt)..."
find "${GIT_CLONE_DIR}/urls/" \( -name '*-sample*.txt' -o -name '*v1_6*' -o -name '*v1_7*' \) -delete -print || true
log "Starting URL filtering and batching..."
SOURCE_URL_FILES_DIR="${GIT_CLONE_DIR}/urls"
for source_txt_file in "${SOURCE_URL_FILES_DIR}/"*.txt; do
    if [ ! -f "$source_txt_file" ]; then
        warn "No .txt files found in ${SOURCE_URL_FILES_DIR} when processing ${source_txt_file}. Skipping."
        continue
    fi
    original_filename_no_ext=$(basename "$source_txt_file" .txt)
    sanitized_source_basename=$(echo "$original_filename_no_ext" | tr -cs 'a-zA-Z0-9_.-' '_')
    log "Processing source file for URL batching: ${original_filename_no_ext} (sanitized to: ${sanitized_source_basename})"
    current_raw_url_batches_dir="${RAW_URL_BATCHES_PARENT_DIR}/allenai_dolma_${sanitized_source_basename}_batches"
    mkdir -p "${current_raw_url_batches_dir}"
    if compgen -G "${current_raw_url_batches_dir}/download_urls_batch_*.txt" > /dev/null; then
        log "Batches for ${sanitized_source_basename} already seem to exist in ${current_raw_url_batches_dir}. Skipping batch generation."
    else
        log "Filtering and chunking URLs from ${source_txt_file} into batches of ${URL_BATCH_SIZE}..."
        grep -E '/c4|/cc_|/falcon-refinedweb' "$source_txt_file" | \
            split -l "${URL_BATCH_SIZE}" --numeric-suffixes=1 -a 5 --additional-suffix=.txt - "${current_raw_url_batches_dir}/download_urls_batch_"
        log "Finished batching ${sanitized_source_basename}. Batches are in ${current_raw_url_batches_dir}"
    fi
done
log "=== STAGE 1 Complete. All source URL files processed for batching. ==="


# --- Stage 2: Process Each Batch (Download Content, Process, Upload) ---
log "=== STAGE 2: Downloading Content and Processing Individual Batches ==="

find "${RAW_URL_BATCHES_PARENT_DIR}" -mindepth 1 -maxdepth 1 -type d | while IFS= read -r source_batch_dir_path; do
    source_dir_name=$(basename "${source_batch_dir_path}" "_batches") # e.g., allenai_dolma_commoncrawl
    log "--- Processing source directory: ${source_dir_name} ---"

    # Per-source directories for outputs and temporary files
    current_temp_wget_dir="${TEMP_WGET_DOWNLOAD_PARENT_DIR}/${source_dir_name}"
    current_processed_parquet_dir="${PROCESSED_PARQUET_PARENT_DIR}/${source_dir_name}"
    current_temp_manifest_dir="${TEMP_MANIFEST_PARENT_DIR}/${source_dir_name}"
    mkdir -p "${current_temp_wget_dir}" "${current_processed_parquet_dir}" "${current_temp_manifest_dir}"

    find "${source_batch_dir_path}" -name 'download_urls_batch_*.txt' | sort -V | while IFS= read -r url_batch_filepath; do
        batch_filename_no_ext=$(basename "${url_batch_filepath}" .txt)
        batch_num_str=$(echo "${batch_filename_no_ext}" | sed -n 's/download_urls_batch_\([0-9]*\)/\1/p')
        batch_num=$((10#$batch_num_str)) # Convert to decimal, robustly

        if [ -z "$batch_num_str" ]; then
            warn "Could not extract batch number from ${batch_filename_no_ext} (File: ${url_batch_filepath}). Skipping."
            continue
        fi

        log "--- Preparing Batch ${batch_num} from ${source_dir_name} (URL File: ${url_batch_filepath}) ---"
        
        # Adjusted state file name to reflect content processing
        state_file_success="${STATE_DIR}/${source_dir_name}_content_batch_${batch_num}.success"
        if [ -f "${state_file_success}" ]; then
            log "Batch ${batch_num} (content) from ${source_dir_name} already processed successfully. Skipping."
            continue
        fi

        # Directory for this specific batch's WGET downloads
        batch_specific_wget_dir="${current_temp_wget_dir}/batch_${batch_num}_downloads"
        # Manifest file for this specific batch
        manifest_filepath="${current_temp_manifest_dir}/batch_${batch_num}_manifest.csv"

        mkdir -p "${batch_specific_wget_dir}"

        # --- Stage 2a: Prepare Manifest and Download URLs with wget ---
        log "[Batch ${batch_num}] Preparing manifest and downloading URL contents..."
        url_counter=0
        # Temporary file to hold arguments for xargs (URL and its target output file path)
        xargs_input_filepath=$(mktemp "${current_temp_manifest_dir}/batch_${batch_num}_xargs_input_XXXXXX.txt")

        if [ ! -s "${url_batch_filepath}" ]; then # Check if the URL list file is empty
            log "[Batch ${batch_num}] URL batch file ${url_batch_filepath} is empty. No content to download."
            touch "${manifest_filepath}" # Create an empty manifest for the Python script
        else
            while IFS= read -r original_url || [ -n "$original_url" ]; do # Process each URL from the batch file
                if [ -z "$original_url" ]; then continue; fi # Skip empty lines

                url_counter=$((url_counter + 1))
                # Define a unique local filename for the downloaded content.
                # Using a simple counter for filename uniqueness within the batch.
                # This avoids issues with special characters in URLs or overly long names.
                # The actual content type/extension isn't critical here as Python reads the raw file.
                local_download_filename_for_wget="url_${url_counter}.data"
                local_download_filepath_for_wget="${batch_specific_wget_dir}/${local_download_filename_for_wget}"

                # Add to manifest CSV: original_url,local_download_filepath_for_wget
                echo "\"$(echo "$original_url" | sed 's/"/""/g')\",\"${local_download_filepath_for_wget}\"" >> "${manifest_filepath}"
                
                # Add to the temporary file for xargs: original_url output_filepath_for_wget
                # Ensure proper quoting if URLs can contain spaces (though typically they don't in the path part).
                printf '%s %s\n' "$original_url" "$local_download_filepath_for_wget" >> "$xargs_input_filepath"
            done < "${url_batch_filepath}"

            if [ $url_counter -gt 0 ]; then
                log "[Batch ${batch_num}] Created manifest with $url_counter URLs. Starting parallel downloads (Jobs: ${WGET_PARALLEL_JOBS})..."

                # Define a shell function for xargs to call for each download
                # This function takes the URL as $1 and the output file path as $2
                run_single_wget() {
                    local url_to_download="$1"
                    local output_filepath="$2"
                    # Use a more descriptive log for individual wget attempts
                    # log "[wget] Attempting: ${url_to_download} -> ${output_filepath}"
                    wget --user-agent="${USER_AGENT}" \
                         --tries="${WGET_TRIES}" \
                         --timeout="${WGET_TIMEOUT}" \
                         --output-document="${output_filepath}" \
                         --no-clobber \
                         "${url_to_download}";
                }
                export -f run_single_wget # Export function for xargs subshells
                export -f log warn # Export logging functions
                export USER_AGENT WGET_TRIES WGET_TIMEOUT # Export relevant variables

                # xargs reads pairs of arguments (url, output_file) from each line of xargs_input_filepath
                # and passes them to run_single_wget.
                # The `_` is a placeholder for the shell name ($0) in the `bash -c` command.
                xargs -a "$xargs_input_filepath" -n 2 -P "${WGET_PARALLEL_JOBS}" bash -c 'run_single_wget "$1" "$2"' _ || true
                # `|| true` ensures the script doesn't exit if some wget calls fail, as Python will handle missing files.

                downloaded_files_count=$(find "${batch_specific_wget_dir}" -type f -size +0c 2>/dev/null | wc -l) # Count non-empty files
                log "[Batch ${batch_num}] Finished download attempts. ${downloaded_files_count} non-empty files found in ${batch_specific_wget_dir}."
            else
                log "[Batch ${batch_num}] No URLs found in ${url_batch_filepath} to download."
                touch "${manifest_filepath}" # Create empty manifest if no URLs
            fi
            rm -f "$xargs_input_filepath" # Clean up temp file for xargs
        fi

        # --- Stage 2b: Process Downloaded Content with Python/DuckDB and Upload to Hugging Face ---
        log "[Batch ${batch_num}] Processing downloaded content using Python/DuckDB (Manifest: ${manifest_filepath})..."
        # The python_source_file_basename is derived from the Dolma source file (e.g., allenai_dolma_commoncrawl)
        python_source_file_basename="${source_dir_name}"

        if ! "${PYTHON_EXEC}" process_batch.py \
            --manifest_file "${manifest_filepath}" \
            --batch_num "${batch_num}" \
            --hf_username "${HF_USERNAME}" \
            --hf_base_dataset_name "${HF_BASE_DATASET_NAME}" \
            ${HF_VARIANT:+--hf_variant "${HF_VARIANT}"} \
            --output_dir "${current_processed_parquet_dir}"; then
            error_exit "[Batch ${batch_num}] Python script (content processing) failed. Manifest: ${manifest_filepath}. Check Python logs."
        fi
        
        processed_parquet_file="${current_processed_parquet_dir}/${python_source_file_basename}_content_batch_${batch_num}.parquet"
        if [ ! -f "${processed_parquet_file}" ]; then
            # This case should ideally be handled by the Python script itself (e.g., creating an empty Parquet).
            # If the Python script is supposed to always create a file, this indicates an unexpected failure.
            error_exit "[Batch ${batch_num}] Python script completed but Parquet file was NOT created: ${processed_parquet_file}"
        fi
        log "[Batch ${batch_num}] Python script (content processing) completed. Parquet file: ${processed_parquet_file}"

        # --- Stage 2c: Sync processed Parquet to S3 ---
        s3_target_path="${S3_BUCKET_BASE_PATH}/" # e.g., s3://.../dolma_processed_content/allenai_dolma_commoncrawl/
        log "[Batch ${batch_num}] Syncing ${processed_parquet_file} to S3 destination: ${s3_target_path}"
        if ! aws s3 cp "${processed_parquet_file}" "${s3_target_path}" --no-follow-symlinks; then
             error_exit "[Batch ${batch_num}] S3 upload failed for ${processed_parquet_file}."
        fi
        log "[Batch ${batch_num}] S3 sync completed for ${processed_parquet_file}."
        
        # --- Stage 2d: Cleanup and State Update ---
        log "[Batch ${batch_num}] Cleaning up local temporary files for this batch..."
        if [ -d "${batch_specific_wget_dir}" ]; then
            log "[Batch ${batch_num}] Removing temporary wget download directory: ${batch_specific_wget_dir}"
            rm -rf "${batch_specific_wget_dir:?}" # Safety: :? ensures var is set and not empty
        fi
        if [ -f "${manifest_filepath}" ];
        then
            log "[Batch ${batch_num}] Removing manifest file: ${manifest_filepath}"
            rm -f "${manifest_filepath}"
        fi
        
        # Optional: Remove the local Parquet file if S3 and HF uploads are successful and you trust them
        # log "[Batch ${batch_num}] Removing local Parquet file: ${processed_parquet_file}"
        # rm -f "${processed_parquet_file}"

        # Mark batch as processed successfully
        touch "${state_file_success}"
        log "--- Successfully processed, archived, and cleaned Content Batch ${batch_num} from ${source_dir_name} ---"
        log "State file created: ${state_file_success}"

    done # End of loop over individual batch files for a source
done   # End of loop over source batch directories

log "=== STAGE 2 Complete. All content batches processed. ==="
log "Pipeline finished successfully."