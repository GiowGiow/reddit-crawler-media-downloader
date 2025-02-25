"""
Utility functions for the Suno Downloader.
"""

import re
from pathlib import Path
from typing import Optional


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
