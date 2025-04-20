#!/usr/bin/env python3
"""
Dataset Processor for Suno Songs

This script:
1. Creates smaller subsets of the dataset for easier sharing
2. Extracts genre information from post titles and adds it as a column
3. Copies the relevant files to new directories
"""

import argparse
import re
import shutil
import sys
from pathlib import Path
from typing import Dict, Optional, Union

import pandas as pd
from tqdm import tqdm


def extract_genre(title: str) -> Optional[str]:
    """
    Extract genre information from post title.

    Args:
        title: Post title

    Returns:
        Genre string if found, None otherwise
    """
    if not title or not isinstance(title, str):
        return None

    # Match genre in square brackets at the beginning of the title
    match = re.match(r"^\s*\[([^\]]+)\]", title)
    if match:
        print(title)
        print(match.group(1).strip())
        return match.group(1).strip()

    return None


def generate_dataset_subsets(
    df: pd.DataFrame,
    base_output_dir: Union[str, Path],
    sizes: Dict[str, int],
    copy_files: bool = True,
) -> Dict[str, pd.DataFrame]:
    """
    Generate subsets of the dataset with different sizes.

    Args:
        df: DataFrame containing the Reddit posts with download paths
        base_output_dir: Base directory to save the subsets
        sizes: Dictionary mapping subset names to their sizes
        copy_files: Whether to copy the files to the new directories

    Returns:
        Dictionary mapping subset names to their DataFrames
    """
    base_output_dir = Path(base_output_dir)

    # Filter out rows without a download path
    has_download = df["download_path"].notna()
    valid_df = df[has_download].copy()

    print(
        f"Found {len(valid_df)} posts with valid download paths out of {len(df)} total posts"
    )

    # Add genre column if it doesn't exist
    if "genre" not in valid_df.columns:
        print("Extracting genre information from post titles...")
        valid_df["genre"] = valid_df["title"].apply(extract_genre)

        # Count genres
        genre_counts = valid_df["genre"].value_counts()
        print(f"Found {genre_counts.count()} unique genres")
        print("Top 10 genres:")
        for genre, count in genre_counts.head(10).items():
            print(f"  {genre}: {count}")

        # Count missing genres
        missing_genres = valid_df["genre"].isna().sum()
        print(f"Missing genres: {missing_genres} ({missing_genres/len(valid_df):.1%})")

    # Create subsets
    subsets = {}

    # Sort by popularity of the post (if score is available, otherwise random)
    if "score" in valid_df.columns:
        sorted_df = valid_df.sort_values("score", ascending=False)
    else:
        sorted_df = valid_df.sample(
            frac=1, random_state=42
        )  # Random order with fixed seed

    # Create each subset
    for name, size in sizes.items():
        if size > len(sorted_df):
            print(
                f"Warning: Requested size {size} for subset '{name}' is larger than available data ({len(sorted_df)} posts)"
            )
            subset_df = sorted_df.copy()
        else:
            subset_df = sorted_df.head(size).copy()

        subset_dir = base_output_dir / name
        subset_dir.mkdir(parents=True, exist_ok=True)

        # Create a modified copy with adjusted paths for this subset
        subset_df_with_local_paths = subset_df.copy()

        # Copy files if requested
        if copy_files:
            print(f"Copying files for subset '{name}'...")
            successful_copies = 0
            failed_copies = 0

            # Create subdirectories
            for subdir in ["reddit", "suno", "others", "youtube", "soundcloud"]:
                (subset_dir / subdir).mkdir(exist_ok=True)

            for idx, row in tqdm(subset_df.iterrows(), total=len(subset_df)):
                if pd.isna(row["download_path"]):
                    continue

                src_path = Path(row["download_path"])

                # Determine destination directory based on original path
                if "reddit" in str(src_path):
                    dst_dir = subset_dir / "reddit"
                    subdir_name = "reddit"
                elif "suno" in str(src_path):
                    dst_dir = subset_dir / "suno"
                    subdir_name = "suno"
                elif "youtube" in str(src_path):
                    dst_dir = subset_dir / "youtube"
                    subdir_name = "youtube"
                elif "soundcloud" in str(src_path):
                    dst_dir = subset_dir / "soundcloud"
                    subdir_name = "soundcloud"
                else:
                    dst_dir = subset_dir / "others"
                    subdir_name = "others"

                dst_path = dst_dir / src_path.name

                # Update the path in the DataFrame to be relative to the subset directory
                subset_df_with_local_paths.at[
                    idx, "download_path"
                ] = f"{subdir_name}/{src_path.name}"

                try:
                    if src_path.exists():
                        shutil.copy2(src_path, dst_path)
                        successful_copies += 1
                    else:
                        failed_copies += 1
                except Exception as e:
                    print(f"  Error copying {src_path}: {e}")
                    failed_copies += 1

            print(
                f"  Copied {successful_copies} files, failed to copy {failed_copies} files"
            )

        # Save the subset metadata with adjusted paths
        subset_path = subset_dir / "metadata.jsonl"
        subset_df_with_local_paths.to_json(subset_path, orient="records", lines=True)
        print(
            f"Created subset '{name}' with {len(subset_df)} posts, saved to {subset_path}"
        )

        subsets[name] = subset_df

    return subsets


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Process dataset to create subsets and extract genre information."
    )

    # Input options
    parser.add_argument(
        "--input",
        default="mapped_posts.jsonl",
        help="Path to JSONL file with mapped Reddit posts (default: mapped_posts.jsonl)",
    )
    parser.add_argument(
        "--output",
        default="datasets",
        help="Base directory to save dataset subsets (default: datasets)",
    )

    # Dataset options
    parser.add_argument(
        "--small",
        type=int,
        default=100,
        help="Size of the small dataset (default: 100)",
    )
    parser.add_argument(
        "--medium",
        type=int,
        default=1000,
        help="Size of the medium dataset (default: 1000)",
    )
    parser.add_argument(
        "--large",
        type=int,
        default=5000,
        help="Size of the large dataset (default: 5000)",
    )
    parser.add_argument(
        "--no-copy",
        action="store_true",
        help="Don't copy files, only create metadata files",
    )
    parser.add_argument(
        "--update-all",
        action="store_true",
        help="Save the full dataset with genre information",
    )

    # Parse arguments
    args = parser.parse_args()

    # Print banner
    print("\n==================================================")
    print("          SUNO DATASET PROCESSOR                 ")
    print("==================================================\n")

    # Load the JSONL file
    print(f"Loading data from {args.input}...")
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"Error: Input file {args.input} does not exist!")
        sys.exit(1)

    df = pd.read_json(input_path, lines=True)
    print(f"Loaded {len(df)} posts")

    # Define dataset sizes
    # sizes = {"small": args.small, "medium": args.medium, "large": args.large}

    # Generate subsets
    # subsets = generate_dataset_subsets(
    #     df, args.output, sizes, copy_files=not args.no_copy
    # )

    # Save the full dataset with genre information if requested
    if args.update_all:
        # Add genre column to full dataset
        print("\nUpdating full dataset with genre information...")
        if "genre" not in df.columns:
            df["genre"] = df["title"].apply(extract_genre)

        # Save to the original file
        df.to_json(input_path, orient="records", lines=True)
        print(f"Saved updated dataset to {input_path}")

    print("\nDone!")


if __name__ == "__main__":
    main()
