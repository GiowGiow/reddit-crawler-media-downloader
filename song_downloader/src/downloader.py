"""
Core downloader class for the Suno Downloader.
"""

import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import requests
from requests.adapters import HTTPAdapter
from suno_downloader.utils import check_existing_file, extract_song_id
from tqdm import tqdm
from urllib3.util import Retry


class SunoDownloader:
    """
    A class for downloading songs from Suno AI.
    """

    def __init__(
        self, output_dir: Union[str, Path] = "downloads", skip_existing: bool = True
    ):
        """
        Initialize the downloader with an output directory.

        Args:
            output_dir: Directory to save downloads
            skip_existing: If True, skip downloads that already exist
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.skip_existing = skip_existing

        # Create subdirectory for suno
        self.output_dir_suno = self.output_dir / "suno"
        self.output_dir_suno.mkdir(exist_ok=True)

        # Set up a requests session with retries
        self.session = requests.Session()
        retries = Retry(
            total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504]
        )
        self.session.mount("http://", HTTPAdapter(max_retries=retries))
        self.session.mount("https://", HTTPAdapter(max_retries=retries))
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            }
        )

    def download_song(self, url: str, filename_prefix: str = "") -> Optional[Path]:
        """
        Download audio from Suno.ai using the simplified method.

        Args:
            url: Original Suno URL
            filename_prefix: Optional prefix for the filename

        Returns:
            Path to the downloaded file or None if failed
        """
        try:
            # Extract the song ID from the URL
            song_id = extract_song_id(url)

            if not song_id:
                print(f"  Could not extract Suno song ID from URL: {url}")
                return None

            # Create a filename based on the song ID and prefix
            if filename_prefix:
                filename = f"{filename_prefix}_{song_id}.mp3"
            else:
                filename = f"{song_id}.mp3"

            filepath = self.output_dir_suno / filename

            # Check if file already exists
            existing = check_existing_file(filepath, self.skip_existing)
            if existing:
                return existing

            # Construct the direct CDN URL
            cdn_url = f"https://cdn1.suno.ai/{song_id}.mp3"

            print(f"  Using direct CDN URL: {cdn_url}")

            # Download the audio file
            response = self.session.get(cdn_url, stream=True)
            if response.status_code == 200:
                print(f"  Downloading audio to: {filepath}")
                with open(filepath, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                return filepath
            else:
                print(f"  Failed to download audio: HTTP {response.status_code}")
                print(f"  URL attempted: {cdn_url}")
                return None

        except Exception as e:
            print(f"  Error downloading Suno audio {url}: {e}")
            return None

    def download_from_url_list(
        self, urls: List[str], sleep_time: float = 0.5
    ) -> Dict[str, Any]:
        """
        Download songs from a list of URLs.

        Args:
            urls: List of URLs to download
            sleep_time: Time to sleep between requests

        Returns:
            Dictionary with download statistics
        """
        results = {"success": 0, "failed": 0, "skipped": 0, "urls": []}

        for url in tqdm(urls, desc="Downloading songs"):
            if not url.strip():
                print("Empty URL, skipping")
                results["skipped"] += 1
                continue

            if "suno.com" not in url:
                print(f"Not a Suno URL: {url}, skipping")
                results["skipped"] += 1
                continue

            print(f"Processing URL: {url}")
            filepath = self.download_song(url)

            url_result = {"url": url, "status": "unknown", "filepath": None}

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
