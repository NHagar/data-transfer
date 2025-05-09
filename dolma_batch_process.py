#!/usr/bin/env python3
import argparse
import logging
import os

import duckdb
import pandas as pd  # For reading the manifest CSV easily
import tldextract
from huggingface_hub import HfApi

# No need for 'import json' if DuckDB handles all JSON parsing

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def extract_domain_udf(url: str) -> str:
    """
    Extracts the registered domain (domain + suffix) from a URL.
    Returns None if the URL is None or the domain cannot be extracted.
    """
    if url is None:
        return None
    try:
        extracted = tldextract.extract(url)
        if not extracted.domain or not extracted.suffix:
            return None
        return f"{extracted.domain}.{extracted.suffix}"
    except Exception:  # Catch any error during tldextract
        return None


def main():
    parser = argparse.ArgumentParser(
        description="Process downloaded JSON files: extract an inner URL from each JSON, get its domain, and save."
    )
    parser.add_argument(
        "--manifest_file",
        required=True,
        help="Path to the manifest CSV file. Expected columns: outer_url, local_json_filepath.",
    )
    parser.add_argument(
        "--batch_num", required=True, type=int, help="The batch number being processed."
    )
    parser.add_argument(
        "--hf_username", required=True, help="Your Hugging Face username."
    )
    parser.add_argument(
        "--hf_base_dataset_name",
        default="dolma_extracted_inner_urls",
        help="Base name for the Hugging Face dataset.",
    )
    parser.add_argument(
        "--hf_variant",
        default=None,
        help="Optional variant name to append to the Hugging Face dataset repository ID.",
    )
    parser.add_argument(
        "--output_dir",
        required=True,
        help="Directory to save the processed Parquet file.",
    )

    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    # Parquet filename reflects the new processing logic
    output_parquet_file = os.path.join(
        args.output_dir,
        f"{args.hf_base_dataset_name}_extracted_inner_urls_batch_{args.batch_num}.parquet",
    )

    logger.info(
        f"Starting JSON processing for batch {args.batch_num} from source {args.hf_base_dataset_name}, using manifest: {args.manifest_file}"
    )
    logger.info(
        f"Inner URL will be extracted from downloaded JSON files using JSONPath: {args.json_inner_url_path}"
    )

    try:
        con = duckdb.connect()
        con.create_function(
            "extract_domain", extract_domain_udf, [str], str, type="SCALAR"
        )

        # Read the manifest file
        manifest_df = pd.read_csv(
            args.manifest_file, header=None, names=["outer_url", "local_json_filepath"]
        )

        fpaths = con.execute("SELECT local_json_filepath FROM manifest_df").fetchall()
        fpaths = [f[0] for f in fpaths]
        fpaths = ",".join(fpaths)

        query = f"""
        WITH urls AS (
            SELECT
                metadata.url AS url
            FROM read_json('[{fpaths}]', format = 'newline_delimited', compression = 'gzip')
        )
        SELECT
            url,
            extract_domain(url) AS domain,
        FROM urls
        WHERE url IS NOT NULL
        """

        logger.debug(f"Executing SQL query for batch {args.batch_num}:\n{query}")
        # Use COPY (query) TO ... for direct writing
        con.execute(f"COPY ({query}) TO '{output_parquet_file}';")
        logger.info(
            f"Successfully processed downloaded JSONs for batch {args.batch_num}, extracted inner URLs, and saved to {output_parquet_file}"
        )

    except Exception as e:
        logger.error(
            f"DuckDB processing failed for batch {args.batch_num} (extracting inner URLs): {e}"
        )
        # Clean up potentially incomplete parquet file if an error occurs mid-process
        if os.path.exists(output_parquet_file):
            os.remove(output_parquet_file)
        raise  # Re-raise the exception to signal failure
    finally:
        if "con" in locals() and con:  # Ensure 'con' was defined
            con.close()

    # Upload the resulting Parquet file to Hugging Face
    upload_to_hf(args, output_parquet_file)


def upload_to_hf(args, parquet_filepath_to_upload):
    """Helper function to upload a file to Hugging Face Hub."""
    try:
        if not os.path.exists(parquet_filepath_to_upload):
            logger.error(
                f"Parquet file {parquet_filepath_to_upload} not found for Hugging Face upload. Skipping upload."
            )
            return

        logger.info(
            f"Pushing file {parquet_filepath_to_upload} (batch {args.batch_num}) to HuggingFace Hub."
        )
        api = HfApi()

        # Construct Hugging Face repository ID
        repo_id_parts = [args.hf_username, args.hf_base_dataset_name]
        if args.hf_variant and args.hf_variant.lower() != "default":
            repo_id_parts.append(args.hf_variant)
        repo_id = "_".join(repo_id_parts)
        repo_id = repo_id.replace("__", "_").strip("_")  # Clean up underscores

        logger.info(f"Target Hugging Face repository ID: {repo_id}")

        api.create_repo(repo_id=repo_id, exist_ok=True, repo_type="dataset")
        path_in_repo = f"data/{os.path.basename(parquet_filepath_to_upload)}"  # Standard to put data in a 'data' subfolder

        api.upload_file(
            path_or_fileobj=parquet_filepath_to_upload,
            path_in_repo=path_in_repo,
            repo_id=repo_id,
            repo_type="dataset",
            commit_message=f"Add extracted inner URLs batch {args.hf_base_dataset_name} batch {args.batch_num}",  # Updated commit message
        )
        logger.info(
            f"Successfully uploaded batch {args.batch_num} to Hugging Face repo {repo_id} as {path_in_repo}"
        )
    except Exception as e:
        logger.error(
            f"Hugging Face upload failed for batch {args.batch_num} ({parquet_filepath_to_upload}): {e}"
        )
        raise  # Re-raise to signal failure to the shell script


if __name__ == "__main__":
    main()
