#!/usr/bin/env python3
"""
Suno Song Downloader

This script downloads songs from Suno AI from Reddit posts in a JSONL file.
It tries yt-dlp first for compatibility with many sites, then falls back to direct downloads.
"""

import argparse
import re
import time
from pathlib import Path
from typing import Any, Dict, Optional, Union
from urllib.parse import urlparse

import pandas as pd
import requests
import yt_dlp as youtube_dl
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util import Retry
import logging

from reddit_scraper.src.utils import parse_arguments
from song_downloader.src.downloader import SunoDownloader
from song_downloader.src.utils import unify_domain

logger = logging.getLogger(__name__)


def download_songs_from_dataframe(
    df: pd.DataFrame,
    output_dir: Union[str, Path] = "dataset",
    max_items: Optional[int] = None,
    skip_existing: bool = True,
    sleep_time: float = 0.5,
) -> pd.DataFrame:
    """
    Process a dataframe of Suno AI posts and download all songs.

    Args:
        df: Pandas DataFrame with Suno AI posts
        output_dir: Directory to save downloads
        max_items: Maximum number of items to download (for testing)
        skip_existing: If True, skip downloads that already exist
        sleep_time: Time to sleep between downloads to avoid rate limiting

    Returns:
        Updated DataFrame with download paths
    """
    downloader = SunoDownloader(output_dir=output_dir, skip_existing=skip_existing)

    # Create a new column for download paths
    df["download_path"] = None
    # Create a column for download status
    df["download_status"] = None

    # Filter to keep only rows that might have audio
    audio_domains = [
        "v.redd.it",
        "youtube.com",
        "suno.com",
        "cdn1.suno.ai",
        "soundcloud.com",
    ]
    potential_audio = df[df["domain_unified"].isin(audio_domains) | df["is_video"]]

    # Limit number of items if specified
    if max_items and max_items > 0:
        potential_audio = potential_audio.head(max_items)

    # Download each post
    for idx, row in tqdm(potential_audio.iterrows(), total=len(potential_audio)):
        post_id = row["id"]
        title = row.get("title", "No title")
        url = row.get("url", "No URL")
        domain = row.get("domain_unified", "Unknown domain")
        permalink = row.get("permalink", None)

        # Construct Reddit URL if permalink exists
        reddit_url = f"https://reddit.com{permalink}" if permalink else "No Reddit URL"

        logger.info(f"Processing [{post_id}] - Domain: {domain}")
        logger.info(f"  Title: {title}")
        logger.info(f"  URL: {url}")
        logger.info(f"  Reddit URL: {reddit_url}")

        # Check if the URL is valid
        if not url or url == "No URL":
            status = "Skipped: No valid URL found"
            logger.info(f"  Status: {status}")
            df.at[idx, "download_status"] = status
            continue

        # Determine appropriate downloader based on domain
        if domain == "v.redd.it":
            logger.info(f"  Using: Reddit video downloader")
            download_path = downloader.download_reddit_video(row)
        elif domain in ["suno.com", "cdn1.suno.ai"]:
            logger.info(f"  Using: Suno audio downloader")
            download_path = downloader.download_suno_audio(url, post_id)
        else:
            # For all other domains, use the generic downloader which tries yt-dlp first
            logger.info(f"  Using: Generic downloader for {domain}")
            download_path = downloader.download_generic_url(url, post_id, domain)

        # Record the download path and status
        if download_path:
            if skip_existing and "skipping download" in str(download_path):
                status = "Skipped: File already exists"
            else:
                status = f"Downloaded to: {download_path}"
            df.at[idx, "download_path"] = str(download_path)
        else:
            status = "Failed: Download was not successful"

        df.at[idx, "download_status"] = status
        logger.info(f"  Status: {status}")
        logger.info("-" * 80)

        # Sleep to avoid rate limiting
        time.sleep(sleep_time)

    # logger.info summary of downloads
    success = df["download_path"].notna().sum()
    failed = len(potential_audio) - success

    logger.info("\nDownload Summary:")
    logger.info(f"  Total processed: {len(potential_audio)}")
    logger.info(f"  Successfully downloaded: {success} ({success/len(potential_audio):.1%})")
    logger.info(f"  Failed: {failed} ({failed/len(potential_audio):.1%})")

    # Group by status for more detailed summary
    if "download_status" in df.columns:
        status_counts = df["download_status"].value_counts()
        logger.info("\nStatus breakdown:")
        for status, count in status_counts.items():
            logger.info(f"  {status}: {count}")

    return df


def main():
    """Main function."""
    args = parse_arguments()

    # logger.info banner
    logger.info("\n==================================================")
    logger.info("           SUNO REDDIT SONG DOWNLOADER            ")
    logger.info("==================================================\n")

    # Load the JSONL file
    logger.info(f"Loading data from {args.input}...")
    input_path = Path(args.input)
    df = pd.read_json(input_path, lines=True)

    # Filter by flairs
    flair_filter = args.flairs
    logger.info(f"Filtering by flairs: {', '.join(flair_filter)}")
    ai_songs = df[df["link_flair_text"].isin(flair_filter)]
    logger.info(f"Found {len(ai_songs)} posts with song flairs")

    # Unify domains
    logger.info("Unifying domains...")
    ai_songs["domain_unified"] = ai_songs["domain"].apply(unify_domain)

    # Display domain counts
    domain_counts = ai_songs["domain_unified"].value_counts()
    logger.info("\nDomain counts:")
    for domain, count in domain_counts.head(10).items():
        logger.info(f"  {domain}: {count}")

    # Download songs
    logger.info("\nDownloading songs...")
    output_dir = Path(args.output)

    # Download with specified parameters
    result_df = download_songs_from_dataframe(
        ai_songs.copy(),
        output_dir=output_dir,
        max_items=args.max,
        skip_existing=not args.force,
        sleep_time=args.sleep,
    )

    # Save the updated dataframe if requested
    if args.save:
        output_df_path = Path(args.save)
        result_df.to_json(output_df_path, orient="records", lines=True)
        logger.info(f"\nUpdated dataframe saved to {output_df_path}")

    logger.info("\nDone!")


if __name__ == "__main__":
    main()
