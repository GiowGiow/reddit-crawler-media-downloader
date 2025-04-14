"""
Input parsing functions for the Suno Downloader.
"""

import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pandas as pd
from suno_downloader.downloader import SunoDownloader
from tqdm import tqdm


def parse_and_download_jsonl(
    downloader: SunoDownloader,
    jsonl_path: Union[str, Path],
    url_field: str = "url",
    id_field: str = "id",
    max_items: Optional[int] = None,
    sleep_time: float = 0.5,
) -> Dict[str, Any]:
    """
    Download songs from a JSONL file containing URLs.

    Args:
        downloader: Initialized SunoDownloader instance
        jsonl_path: Path to JSONL file
        url_field: Field name containing the URL
        id_field: Field name containing the ID
        max_items: Maximum number of items to download
        sleep_time: Time to sleep between requests

    Returns:
        Dictionary with download statistics
    """
    # Load the JSONL file
    df = pd.read_json(jsonl_path, lines=True)

    # Create results dictionary
    results = {"success": 0, "failed": 0, "skipped": 0, "urls": []}

    # Filter to only Suno URLs
    suno_df = df[df[url_field].str.contains("suno.com", na=False)]
    print(f"Found {len(suno_df)} Suno URLs in JSONL file")

    # Limit number of items if specified
    if max_items and max_items > 0:
        suno_df = suno_df.head(max_items)
        print(f"Processing first {max_items} Suno URLs")

    # Process each URL
    for idx, row in tqdm(
        suno_df.iterrows(), total=len(suno_df), desc="Downloading songs"
    ):
        url = row.get(url_field, "")
        post_id = row.get(id_field, "")

        if not url:
            print("Empty URL, skipping")
            results["skipped"] += 1
            continue

        print(f"Processing URL: {url}")
        filepath = downloader.download_song(url, post_id)

        url_result = {"url": url, "id": post_id, "status": "unknown", "filepath": None}

        if filepath:
            if "skipping download" in str(filepath):
                url_result["status"] = "skipped"
                results["skipped"] += 1
            else:
                url_result["status"] = "success"
                url_result["filepath"] = str(filepath)
                results["success"] += 1
        else:
            url_result["status"] = "failed"
            results["failed"] += 1

        results["urls"].append(url_result)

        # Sleep to avoid rate limiting
        time.sleep(sleep_time)

    return results


def parse_url_file(file_path: Union[str, Path]) -> List[str]:
    """
    Parse a file containing a list of URLs, one per line.

    Args:
        file_path: Path to the file

    Returns:
        List of URLs
    """
    with open(file_path, "r") as f:
        return [line.strip() for line in f if line.strip()]
