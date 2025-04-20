#!/usr/bin/env python3
"""
Suno Song Downloader

This script downloads songs from Suno AI from Reddit posts in a JSONL file.
It tries yt-dlp first for compatibility with many sites, then falls back to direct downloads.
"""


import logging
from collections import namedtuple
from pathlib import Path

from song_downloader.src.downloader import download_songs_from_dataframe
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
