import argparse

from huggingface_hub import snapshot_download

p = argparse.ArgumentParser(description="Mirror a HuggingFace repo locally")
p.add_argument("repo_id", help="e.g. 'bigscience-data/the-pile'")
p.add_argument("local_dir")

args = p.parse_args()

repo_id = args.repo_id
loc = repo_id.split("/")[-1]

snapshot_download(
    repo_id=repo_id,
    repo_type="dataset",
    local_dir=args.local_dir,
    local_dir_use_symlinks=False,
    allow_patterns="*.parquet",  # filter to the heavy bits
    max_workers=32,  # saturate 10–25 Gb/s
)
