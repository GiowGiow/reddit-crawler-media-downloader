#!/usr/bin/env python3
"""
Map Downloaded Files to Reddit Posts

This script maps already downloaded files in the dataset directory
back to the original Reddit posts without attempting to download again.
"""

import argparse
import re
import sys
from pathlib import Path
from typing import Dict, Union

import pandas as pd
from tqdm import tqdm

from song_downloader.src.utils import unify_domain


def sanitize_filename(filename: str) -> str:
    """Sanitize a filename to remove invalid characters."""
    return re.sub(r'[\\/*?:"<>|]', "_", filename)


def get_all_downloaded_files(base_dir: Union[str, Path]) -> Dict[str, Path]:
    """
    Scan the download directory for all downloaded files.

    Args:
        base_dir: Base directory where files were downloaded

    Returns:
        Dictionary mapping post_ids to file paths
    """
    base_dir = Path(base_dir)

    # Create a dictionary to map post IDs to file paths
    id_to_path: Dict[str, Path] = {}

    # Define subdirectories to check based on the structure in your project
    subdirs = ["reddit", "suno", "others"]
    for platform in ["youtube", "soundcloud", "spotify"]:
        if (base_dir / platform).exists():
            subdirs.append(platform)

    print(f"Scanning for files in {base_dir}...")
    total_files = 0

    for subdir in subdirs:
        dir_path = base_dir / subdir
        if not dir_path.exists():
            print(f"Directory {dir_path} doesn't exist, skipping...")
            continue

        # Count files in this directory
        files_in_dir = list(dir_path.glob("**/*.*"))
        total_files += len(files_in_dir)

        print(f"Scanning {dir_path} ({len(files_in_dir)} files)...")
        for file_path in tqdm(files_in_dir, desc=f"Processing {subdir}"):
            if file_path.is_file():
                # Extract post ID from filename
                filename = file_path.name

                # Try different patterns to extract post ID

                # Pattern 1: post_id.mp3 (used for Suno)
                match = re.match(r"^([a-z0-9_]+)\..*$", filename)
                if match:
                    post_id = match.group(1)
                    id_to_path[post_id] = file_path
                    continue

                # Pattern 2: post_id_title.ext (used for some files)
                match = re.match(r"^([a-z0-9_]+)_.*\..*$", filename)
                if match:
                    post_id = match.group(1)
                    id_to_path[post_id] = file_path
                    continue

                # Pattern 3: For Reddit videos which use the format post_id_title.mp4
                match = re.match(r"^([a-z0-9]+)_.*\.mp4$", filename)
                if match:
                    post_id = match.group(1)
                    id_to_path[post_id] = file_path
                    continue

    print(
        f"Found {len(id_to_path)} downloaded files with extractable post IDs out of {total_files} total files"
    )
    return id_to_path


def map_files_to_posts(
    df: pd.DataFrame,
    downloaded_files: Dict[str, Path],
    output_dir: Union[str, Path] = "dataset",
) -> pd.DataFrame:
    """
    Map downloaded files to their corresponding posts in the DataFrame.

    Args:
        df: DataFrame containing the Reddit posts
        downloaded_files: Dictionary mapping post IDs to file paths
        output_dir: Base directory where files were downloaded

    Returns:
        Updated DataFrame with download paths
    """
    # Add columns for download path and status if they don't exist
    if "download_path" not in df.columns:
        df["download_path"] = None
    if "download_status" not in df.columns:
        df["download_status"] = None

    # Track which files we've matched
    matched_post_ids = set()
    matched_count = 0
    previously_matched = 0

    # Convert output_dir to Path for consistency
    output_dir = Path(output_dir)

    # Count already mapped entries
    for idx, row in df.iterrows():
        if pd.notna(df.at[idx, "download_path"]):
            previously_matched += 1

    if previously_matched > 0:
        print(f"Found {previously_matched} posts that already have download paths")

    # Iterate through each row in the DataFrame
    print("Mapping files to posts...")
    for idx, row in tqdm(df.iterrows(), total=len(df)):
        post_id = row["id"]

        # Skip if already has a download path
        if pd.notna(df.at[idx, "download_path"]):
            matched_count += 1
            matched_post_ids.add(post_id)
            continue

        # Check if we have a downloaded file for this post ID
        if post_id in downloaded_files:
            file_path = downloaded_files[post_id]
            # Make path relative to the current directory for better portability
            rel_path = (
                file_path.relative_to(Path.cwd())
                if Path.cwd() in file_path.parents
                else file_path
            )

            df.at[idx, "download_path"] = str(rel_path)
            df.at[idx, "download_status"] = f"Downloaded to: {rel_path}"
            matched_count += 1
            matched_post_ids.add(post_id)

    # Print summary
    new_matches = matched_count - previously_matched
    print("\nMapping Summary:")
    print(f"  Total posts: {len(df)}")
    print(f"  Previously mapped: {previously_matched}")
    print(f"  Newly mapped: {new_matches}")
    print(
        f"  Total matched with downloads: {matched_count} ({matched_count/len(df):.1%})"
    )
    print(
        f"  Unmatched: {len(df) - matched_count} ({(len(df) - matched_count)/len(df):.1%})"
    )
    print(f"  Unused downloaded files: {len(downloaded_files) - len(matched_post_ids)}")

    return df


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Map downloaded files to Reddit posts without downloading again."
    )

    # Input options
    parser.add_argument(
        "--input",
        default="../reddit_scraper/reddit_data/r_sunoai_posts.jsonl",
        help="Path to JSONL file with Reddit posts (default: ../reddit_scraper/reddit_data/r_sunoai_posts.jsonl)",
    )
    parser.add_argument(
        "--dataset-dir",
        default="dataset",
        help="Directory where files were downloaded (default: dataset)",
    )
    parser.add_argument(
        "--save",
        default="mapped_posts.jsonl",
        help="Path to save the updated dataframe (default: mapped_posts.jsonl)",
    )

    # Parse arguments
    args = parser.parse_args()

    # Print banner
    print("\n==================================================")
    print("        MAPPING DOWNLOADED FILES TO POSTS         ")
    print("==================================================\n")

    # Load the JSONL file
    print(f"Loading data from {args.input}...")
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"Error: Input file {args.input} does not exist!")
        sys.exit(1)

    df = pd.read_json(input_path, lines=True)
    print(f"Loaded {len(df)} posts")

    # Ensure domain_unified column exists
    if "domain_unified" not in df.columns:
        print("Unifying domains...")
        df["domain_unified"] = df["domain"].apply(unify_domain)

    # Check if dataset directory exists
    dataset_dir = Path(args.dataset_dir)
    if not dataset_dir.exists():
        print(f"Error: Dataset directory {args.dataset_dir} does not exist!")
        sys.exit(1)

    # Get all downloaded files
    downloaded_files = get_all_downloaded_files(args.dataset_dir)

    # Map files to posts
    result_df = map_files_to_posts(df, downloaded_files, args.dataset_dir)

    # Save the updated DataFrame
    output_path = Path(args.save)
    result_df.to_json(output_path, orient="records", lines=True)
    print(f"\nUpdated dataframe saved to {output_path}")

    print("\nDone!")


if __name__ == "__main__":
    main()
