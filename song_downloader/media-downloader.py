#!/usr/bin/env python3
"""
Suno Song Downloader

This script downloads songs from Suno AI from Reddit posts in a JSONL file.
It tries yt-dlp first for compatibility with many sites, then falls back to direct downloads.
"""


import logging
import time
from pathlib import Path
from typing import Optional, Union
from urllib.parse import urljoin

import pandas as pd
from tqdm import tqdm

from song_downloader.src.constants import AudioDomainType
from song_downloader.src.downloader import MusicDownloader
from song_downloader.src.utils import (
    diplay_domains_counts_cli,
    filter_by_flairs,
    load_jsonl_posts,
    parse_arguments,
    unify_domain,
)

logger = logging.getLogger(__name__)


def download_songs_from_dataframe(
    suno_ai_posts_df: pd.DataFrame,
    output_dir: Union[str, Path] = "dataset",
    max_items_to_download: Optional[int] = None,
    skip_existing: bool = True,
    sleep_time: float = 0.5,
) -> pd.DataFrame:
    """
    Process a dataframe of Suno AI posts and download all songs.

    Args:
        suno_ai_posts_df: Pandas DataFrame with Suno AI posts
        output_dir: Directory to save downloads
        max_items_to_download: Maximum number of items to download (for testing)
        skip_existing: If True, skip downloads that already exist
        sleep_time: Time to sleep between downloads to avoid rate limiting

    Returns:
        Updated DataFrame with download paths
    """
    downloader = MusicDownloader(output_dir=output_dir, skip_existing=skip_existing)

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

    # Create lists to store updates for batch processing
    indices = []
    download_paths = []
    download_statuses = []

    # Download each post
    for idx, row in tqdm(potential_audio.iterrows(), total=len(potential_audio)):
        post_id = row.get("id")
        title = row.get("title", "No title")
        url = row.get("url", "No URL")
        domain = row.get("domain_unified", "Unknown domain")
        permalink = row.get("permalink", None)

        # Construct Reddit URL if permalink exists
        reddit_url = (
            urljoin("https://reddit.com", permalink) if permalink else "No Reddit URL"
        )

        logger.info(f"Processing [{post_id}] - Domain: {domain}")
        logger.info(f"  Title: {title}")
        logger.info(f"  URL: {url}")
        logger.info(f"  Reddit URL: {reddit_url}")

        # Check if the URL is valid
        if not url or url == "No URL":
            status = "Skipped: No valid URL found"
            logger.info(f"  Status: {status}")
            indices.append(idx)
            download_paths.append(None)
            download_statuses.append(status)
            continue

        download_path = downloader.download_by_domain(row)

        # Record the download path and status
        if download_path:
            if skip_existing and "skipping download" in str(download_path):
                status = "Skipped: File already exists"
            else:
                status = f"Downloaded to: {download_path}"
            path_str = str(download_path)
        else:
            status = "Failed: Download was not successful"
            path_str = None

        # Store updates for batch processing
        indices.append(idx)
        download_paths.append(path_str)
        download_statuses.append(status)

        logger.info(f"  Status: {status}")
        logger.info("-" * 80)

        # Sleep to avoid rate limiting
        time.sleep(sleep_time)

    # Apply all updates in a batch
    suno_ai_posts_df.loc[indices, "download_path"] = download_paths
    suno_ai_posts_df.loc[indices, "download_status"] = download_statuses

    # logger.info summary of downloads
    success = suno_ai_posts_df["download_path"].notna().sum()
    failed = len(potential_audio) - success

    logger.info("\nDownload Summary:")
    logger.info(f"  Total processed: {len(potential_audio)}")
    logger.info(
        f"  Successfully downloaded: {success} ({success/len(potential_audio):.1%})"
    )
    logger.info(f"  Failed: {failed} ({failed/len(potential_audio):.1%})")

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
    )

    output_df_path = Path(args.save)
    result_df.to_json(output_df_path, orient="records", lines=True)
    logger.info(f"\nUpdated dataframe saved to {output_df_path}")

    logger.info("\nDone!")


if __name__ == "__main__":
    main()
