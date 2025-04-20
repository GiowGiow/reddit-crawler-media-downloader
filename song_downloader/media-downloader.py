#!/usr/bin/env python3
"""
Suno Song Downloader

This script downloads songs from Suno AI from Reddit posts in a JSONL file.
It tries yt-dlp first for compatibility with many sites, then falls back to direct downloads.
"""


from collections import namedtuple
import logging
import time
from pathlib import Path
from typing import Optional, Union
import concurrent.futures
from threading import Lock

import pandas as pd
from tqdm import tqdm

from song_downloader.src.constants import AudioDomainType
from song_downloader.src.downloader import MusicDownloader
from song_downloader.src.post_processor import DownloadStatus, PostProcessor
from song_downloader.src.utils import (
    diplay_domains_counts_cli,
    filter_by_flairs,
    load_jsonl_posts,
    parse_arguments,
    unify_domain,
)

logger = logging.getLogger(__name__)

# Define a named tuple for download results
DownloadResult = namedtuple("DownloadResult", ["index", "path", "status"])


def download_worker(args):
    """
    Worker function for downloading a single post.

    Args:
        args: Tuple containing (processor, idx, row)

    Returns:
        DownloadResult: Result of the download operation
    """
    processor, idx, row = args
    try:
        path_str, status = processor.process_post(row)
        return DownloadResult(idx, path_str, status.value)
    except Exception as e:
        logger.error(f"Error processing post {row.get('id', 'unknown')}: {str(e)}")
        error_message = DownloadStatus.ERROR.value + f": {str(e)}"
        return DownloadResult(idx, None, error_message)


def download_songs_from_dataframe(
    suno_ai_posts_df: pd.DataFrame,
    output_dir: Union[str, Path] = "dataset",
    max_items_to_download: Optional[int] = None,
    skip_existing: bool = True,
    sleep_time: float = 0,
    num_workers: int = 1,
) -> pd.DataFrame:
    """
    Process a dataframe of Suno AI posts and download all songs.

    Args:
        suno_ai_posts_df: Pandas DataFrame with Suno AI posts
        output_dir: Directory to save downloads
        max_items_to_download: Maximum number of items to download (for testing)
        skip_existing: If True, skip downloads that already exist
        sleep_time: Time to sleep between downloads to avoid rate limiting
        num_workers: Number of parallel download workers

    Returns:
        Updated DataFrame with download paths
    """
    downloader = MusicDownloader(output_dir=output_dir, skip_existing=skip_existing)
    processor = PostProcessor(downloader)

    # Initialize columns for download paths and status
    suno_ai_posts_df["download_path"] = None
    suno_ai_posts_df["download_status"] = None

    # Filter to keep only rows that might have audio
    audio_domains = AudioDomainType.get_all_domains()
    potential_audio = suno_ai_posts_df[
        suno_ai_posts_df["domain_unified"].isin(audio_domains)
        | suno_ai_posts_df["is_video"]
    ]

    # Limit number of items if specified
    if max_items_to_download and max_items_to_download > 0:
        potential_audio = potential_audio.head(max_items_to_download)

    download_results: list[DownloadResult] = []

    # Use ThreadPoolExecutor for parallel downloads
    if num_workers > 1:
        # Prepare arguments for the worker function
        worker_args = [(processor, idx, row) for idx, row in potential_audio.iterrows()]

        # Use ThreadPoolExecutor to download in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = [executor.submit(download_worker, arg) for arg in worker_args]

            # Create a progress bar
            with tqdm(total=len(futures), desc="Downloading") as progress_bar:
                for future in concurrent.futures.as_completed(futures):
                    result = future.result()
                    download_results.append(result)
                    progress_bar.update(1)
                    # Sleep if needed (but this will sleep between worker completions)
                    if sleep_time > 0:
                        time.sleep(sleep_time / num_workers)
    else:
        # Original sequential implementation
        for idx, row in tqdm(potential_audio.iterrows(), total=len(potential_audio)):
            try:
                path_str, status = processor.process_post(row)
                download_results.append(DownloadResult(idx, path_str, status.value))
            except Exception as e:
                logger.error(
                    f"Error processing post {row.get('id', 'unknown')}: {str(e)}"
                )
                error_message = DownloadStatus.ERROR.value + f": {str(e)}"
                download_results.append(DownloadResult(idx, None, error_message))

            # Sleep to avoid rate limiting
            time.sleep(sleep_time)

    # Apply all updates in a batch
    for result in download_results:
        suno_ai_posts_df.loc[result.index, "download_path"] = result.path
        suno_ai_posts_df.loc[result.index, "download_status"] = result.status

    # Log summary of downloads
    success = suno_ai_posts_df["download_path"].notna().sum()
    processor.log_download_summary(len(potential_audio), success)

    # Group by status for more detailed summary
    if "download_status" in suno_ai_posts_df.columns:
        status_counts = suno_ai_posts_df["download_status"].value_counts()
        logger.info("\nStatus breakdown:")
        for status, count in status_counts.items():
            logger.info(f"  {status}: {count}")

    return suno_ai_posts_df


def main():
    """Main function."""
    args = parse_arguments()

    # logger.info banner
    logger.info("\n==================================================")
    logger.info("           REDDIT SONG DOWNLOADER            ")
    logger.info("==================================================\n")

    # Load the JSONL file
    reddit_posts_df = load_jsonl_posts(args)

    # Filter by flairs
    ai_songs = filter_by_flairs(args, reddit_posts_df)

    # Unify domains
    logger.info("Unifying domains...")
    ai_songs["domain_unified"] = ai_songs["domain"].apply(unify_domain)

    # Display domain counts
    diplay_domains_counts_cli(ai_songs)

    # Download songs
    logger.info("\nDownloading songs...")
    output_dir = Path(args.output)

    # Download with specified parameters
    result_df = download_songs_from_dataframe(
        ai_songs.copy(),
        output_dir=output_dir,
        max_items_to_download=args.max,
        skip_existing=not args.force,
        sleep_time=args.sleep,
        num_workers=args.workers,
    )

    output_df_path = Path(args.save)
    result_df.to_json(output_df_path, orient="records", lines=True)
    logger.info(f"\nUpdated dataframe saved to {output_df_path}")

    logger.info("\nDone!")


if __name__ == "__main__":
    main()
