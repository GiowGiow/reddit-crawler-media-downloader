"""
Utility functions for the Suno Downloader.
"""

import argparse
import logging
import re
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


def sanitize_filename(filename: str) -> str:
    """
    Sanitize a filename to remove invalid characters.

    Args:
        filename: The filename to sanitize

    Returns:
        Sanitized filename
    """
    # Replace invalid characters with underscores
    return re.sub(r'[\\/*?:"<>|]', "_", filename)


def check_existing_file(filepath: Path, skip_existing: bool) -> Optional[Path]:
    """
    Check if a file already exists and return it if skip_existing is True.

    Args:
        filepath: Path to check
        skip_existing: Whether to skip existing files

    Returns:
        Path if file exists and should be skipped, None otherwise
    """
    if filepath.exists() and skip_existing:
        print(f"  Found existing file: {filepath}, skipping download")
        return filepath
    return None


def extract_song_id(url: str) -> Optional[str]:
    """
    Extract a Suno song ID from a URL.

    Args:
        url: The URL to extract from

    Returns:
        Song ID if found, None otherwise
    """
    # Try to extract from URL path first
    # URL pattern: https://suno.com/song/{song_id}
    parts = url.strip("/").split("/")

    for i, part in enumerate(parts):
        if part == "song" and i + 1 < len(parts):
            song_id = parts[i + 1]
            # Validate that it looks like a UUID
            if re.match(
                r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
                song_id,
            ):
                return song_id

    # If that fails, try to extract UUID pattern from anywhere in the URL
    match = re.search(
        r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})",
        url,
    )
    if match:
        return match.group(1)

    return None


def unify_domain(domain: str) -> str:
    """
    Unify domain names for better categorization.

    Args:
        domain: The domain to unify

    Returns:
        Unified domain name
    """
    if not domain:
        return "N/A"
    domain_lower_case = domain.lower().strip()
    # unify youtube
    if domain_lower_case in [
        "youtube.com",
        "youtu.be",
        "m.youtube.com",
        "music.youtube.com",
    ]:
        return "youtube.com"
    # unify soundcloud
    if domain_lower_case in ["soundcloud.com", "m.soundcloud.com", "on.soundcloud.com"]:
        return "soundcloud.com"
    # unify X/Twitter
    if domain_lower_case == "x.com":
        return "twitter.com"
    # handle empty domains
    if not domain_lower_case:
        return "N/A"
    # for everything else, just return as is
    return domain_lower_case


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Download songs from the SunoAI subreddit."
    )

    # Input options
    parser.add_argument(
        "--input", required=True, help="Path to JSONL file with Reddit posts"
    )
    parser.add_argument(
        "--output", default="dataset", help="Output directory for downloads"
    )

    # Filter options
    parser.add_argument(
        "--flairs",
        nargs="+",
        default=[
            "Song - Audio Upload",
            "Song - Human Written Lyrics",
            "Song",
            "Meme Song",
        ],
        help="List of flairs to filter by",
    )

    # Download options
    parser.add_argument("--max", type=int, help="Maximum number of items to download")
    parser.add_argument(
        "--force", action="store_true", help="Force re-download of existing files"
    )
    parser.add_argument(
        "--sleep", type=float, default=0.5, help="Sleep time between requests"
    )

    # Output options
    parser.add_argument(
        "--save", help="Save the updated dataframe to JSONL file", default=True
    )

    # Parse arguments
    args = parser.parse_args()
    return args


def filter_by_flairs(args, df):
    flair_filter = args.flairs
    logger.info(f"Filtering by flairs: {', '.join(flair_filter)}")
    ai_songs = df[df["link_flair_text"].isin(flair_filter)]
    logger.info(f"Found {len(ai_songs)} posts with song flairs")
    return ai_songs


def load_jsonl_posts(args):
    logger.info(f"Loading data from {args.input}...")
    input_path = Path(args.input)
    df = pd.read_json(input_path, lines=True)
    return df


def diplay_domains_counts_cli(ai_songs):
    domain_counts = ai_songs["domain_unified"].value_counts()
    logger.info("\nDomain counts:")
    for domain, count in domain_counts.head(10).items():
        logger.info(f"  {domain}: {count}")
