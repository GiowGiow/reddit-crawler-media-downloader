#!/usr/bin/env python3
"""
Python adapted code to download Reddit data using Arctic Shift API.

More on Arctic Shift API: https://arctic-shift.photon-reddit.com
"""
import logging
import os
import sys

from reddit_scraper.src.constants import API_URL
from reddit_scraper.src.downloader import (
    ArchiveStream,
    CombinedArchiveStream,
    DownloadType,
)
from reddit_scraper.src.utils import (
    number_to_short_str,
    parse_arguments,
    parse_dates,
    validate_subreddit_or_username_exists,
)

logger = logging.getLogger(__name__)


def main():
    args = parse_arguments()

    # If neither posts nor comments specified, default to both
    if not args.posts and not args.comments:
        args.posts = True
        args.comments = True

    # Convert type to enum
    download_type = (
        DownloadType.SUBREDDIT if args.type == "subreddit" else DownloadType.USER
    )

    try:
        # Validate the name and get info
        start_timestamp, info = validate_subreddit_or_username_exists(
            args.name, download_type
        )

        # Display info
        if download_type == DownloadType.SUBREDDIT:
            entity_type = "Subreddit"
            entity_prefix = "r/"
        else:
            entity_type = "User"
            entity_prefix = "u/"

        logger.info(f"Found {entity_type}: {entity_prefix}{args.name}")

        # Show approximate counts if available
        if info and "_meta" in info:
            meta = info["_meta"]
            logger.info(
                f"Approximately {number_to_short_str(meta.get('num_posts', 0))} posts and "
                f"{number_to_short_str(meta.get('num_comments', 0))} comments"
            )

        # Parse dates
        start_timestamp, end_timestamp = parse_dates(args)

        # Create output directory if it doesn't exist
        os.makedirs(args.output_dir, exist_ok=True)

        # Set up file prefixes and URLs
        file_prefix = "r_" if download_type == DownloadType.SUBREDDIT else "u_"
        file_prefix += args.name

        end_date_condition = f"&before={end_timestamp}" if end_timestamp else ""

        # Create streams
        posts_stream = None
        comments_stream = None

        if args.posts:
            posts_file = os.path.join(args.output_dir, f"{file_prefix}_posts.jsonl")
            posts_url = f"{API_URL}/api/posts/search?{download_type.value}={args.name}{end_date_condition}"
            posts_stream = ArchiveStream(
                posts_url, start_timestamp, posts_file, "Posts", args.append
            )

        if args.comments:
            comments_file = os.path.join(
                args.output_dir, f"{file_prefix}_comments.jsonl"
            )
            comments_url = f"{API_URL}/api/comments/search?{download_type.value}={args.name}{end_date_condition}"
            comments_stream = ArchiveStream(
                comments_url, start_timestamp, comments_file, "Comments", args.append
            )

        # Start the downloads
        combined_stream = CombinedArchiveStream(posts_stream, comments_stream)
        combined_stream.start()

    except Exception as e:
        logger.info(f"Error: {str(e)}")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
