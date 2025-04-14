"""
Core downloader class for the Suno Downloader.
"""

import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import requests
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util import Retry
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

logger = logging.getLogger(__name__)


class MusicDownloader:
    def __init__(
        self, output_dir: Union[str, Path] = "dataset", skip_existing: bool = True
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

        # Create subdirectories for different sources
        self.dirs: Dict[str, Path] = {
            "reddit": self.output_dir / "reddit",
            "suno": self.output_dir / "suno",
            "others": self.output_dir / "others",
        }

        for directory in self.dirs.values():
            directory.mkdir(exist_ok=True)

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

        # Base youtube-dl options
        self.ydl_opts = {
            "format": "bestaudio/best",
            "postprocessors": [
                {
                    "key": "FFmpegExtractAudio",
                    "preferredcodec": "mp3",
                    "preferredquality": "192",
                }
            ],
            "quiet": True,
            "no_warnings": True,
            "nocheckcertificate": True,
            "ignoreerrors": True,
            "no_color": True,
            "geo_bypass": True,
            "retries": 10,
            "fragment_retries": 10,
        }

    def sanitize_filename(self, filename: str) -> str:
        """Sanitize a filename to remove invalid characters."""
        # Replace invalid characters with underscores
        return re.sub(r'[\\/*?:"<>|]', "_", filename)

    def check_existing_file(self, filepath: Path) -> Optional[Path]:
        """
        Check if a file already exists and return it if skip_existing is True.

        Args:
            filepath: Path to check

        Returns:
            Path if file exists and should be skipped, None otherwise
        """
        if filepath.exists() and self.skip_existing:
            logger.info(f"  Found existing file: {filepath}, skipping download")
            return filepath
        return None

    def download_reddit_video(self, post_data: Dict[str, Any]) -> Optional[Path]:
        """Download a Reddit video using direct download."""
        post_id = post_data["id"]
        title = self.sanitize_filename(post_data.get("title", post_id))
        filename = f"{post_id}_{title[:50]}.mp4"
        filepath = self.dirs["reddit"] / filename

        # Check if file already exists
        existing = self.check_existing_file(filepath)
        if existing:
            return existing

        try:
            # Direct download if we have video information
            if (
                post_data.get("is_video", False)
                and post_data.get("secure_media")
                and post_data["secure_media"].get("reddit_video")
            ):
                video_url = post_data["secure_media"]["reddit_video"].get(
                    "fallback_url"
                )
                if not video_url:
                    logger.info(f"  No fallback URL found in post data")
                    return None

                logger.info(f"  Downloading directly: {video_url}")
                # For reddit videos, download directly
                response = self.session.get(video_url, stream=True)
                if response.status_code == 200:
                    logger.info(f"  Direct download successful, saving to: {filepath}")
                    with open(filepath, "wb") as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)
                    return filepath
                else:
                    logger.info(
                        f"  Direct download failed with status code: {response.status_code}"
                    )
            else:
                logger.info(f"  No video information found in post data")

        except Exception as e:
            logger.info(f"  Error downloading Reddit video: {e}")

        return None

    def download_suno_audio(self, url: str, post_id: str) -> Optional[Path]:
        """
        Download audio from Suno.ai using the simplified method.

        Args:
            url: Original Suno URL
            post_id: Post ID for the filename

        Returns:
            Path to the downloaded file or None if failed
        """
        # Create a filename based on the post ID
        filename = f"{post_id}.mp3"
        filepath = self.dirs["suno"] / filename

        # Check if file already exists
        existing = self.check_existing_file(filepath)
        if existing:
            return existing

        try:
            # Extract the song ID from the URL
            # URL pattern: https://suno.com/song/{song_id}
            parsed_url = urlparse(url)
            path_parts = parsed_url.path.strip("/").split("/")

            # If the URL format is as expected
            if len(path_parts) >= 2 and path_parts[0] == "song":
                song_id = path_parts[1]
                # Construct the direct CDN URL
                cdn_url = f"https://cdn1.suno.ai/{song_id}.mp3"

                logger.info(f"  Using direct CDN URL: {cdn_url}")

                # Download the audio file
                response = self.session.get(cdn_url, stream=True)
                if response.status_code == 200:
                    logger.info(f"  Downloading audio to: {filepath}")
                    with open(filepath, "wb") as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)
                    return filepath
                else:
                    logger.info(f"  Failed to download audio: HTTP {response.status_code}")
                    logger.info(f"  URL attempted: {cdn_url}")
            else:
                # If we can't extract the song ID from the URL, try to get it from the URL itself
                match = re.search(
                    r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})",
                    url,
                )
                if match:
                    song_id = match.group(1)
                    cdn_url = f"https://cdn1.suno.ai/{song_id}.mp3"

                    logger.info(f"  Using direct CDN URL (from regex): {cdn_url}")

                    # Download the audio file
                    response = self.session.get(cdn_url, stream=True)
                    if response.status_code == 200:
                        logger.info(f"  Downloading audio to: {filepath}")
                        with open(filepath, "wb") as f:
                            for chunk in response.iter_content(chunk_size=8192):
                                f.write(chunk)
                        return filepath
                    else:
                        logger.info(
                            f"  Failed to download audio: HTTP {response.status_code}"
                        )
                        logger.info(f"  URL attempted: {cdn_url}")
                else:
                    logger.info(f"  Could not extract Suno song ID from URL: {url}")
        except Exception as e:
            logger.info(f"  Error downloading Suno audio {url}: {e}")

        return None

    def download_generic_url(
        self, url: str, post_id: str, domain: str
    ) -> Optional[Path]:
        """
        Download from a generic URL using yt-dlp first, then falling back to direct download.

        Args:
            url: URL to download from
            post_id: Post ID for the filename
            domain: Domain for categorization

        Returns:
            Path to the downloaded file or None if failed
        """
        # Determine the output directory and filename
        domain_dir = self.dirs.get(domain, self.dirs["others"])

        # Create a base filename without extension (yt-dlp will add the appropriate extension)
        base_filename = f"{self.sanitize_filename(post_id)}"
        filepath_base = domain_dir / base_filename

        # For the direct download fallback, we need a filename with extension
        parsed_url = urlparse(url)
        url_filename = Path(parsed_url.path).name
        if url_filename and "." in url_filename:
            # Use the filename from the URL if it has an extension
            direct_filename = f"{self.sanitize_filename(post_id)}_{url_filename}"
        else:
            # Default to mp3 if no extension can be determined
            direct_filename = f"{self.sanitize_filename(post_id)}.mp3"

        filepath_direct = domain_dir / direct_filename

        # Check if either file already exists
        # We need to check both possible filenames
        existing_files = list(domain_dir.glob(f"{base_filename}.*"))
        if existing_files and self.skip_existing:
            existing_file = existing_files[0]
            logger.info(f"  Found existing file: {existing_file}, skipping download")
            return existing_file

        existing = self.check_existing_file(filepath_direct)
        if existing:
            return existing

        # First attempt: Try yt-dlp as it supports many sites
        logger.info(f"  Trying yt-dlp for {url}...")
        try:
            # Configure yt-dlp options
            ydl_opts = {
                "format": "bestaudio/best",
                "postprocessors": [
                    {
                        "key": "FFmpegExtractAudio",
                        "preferredcodec": "mp3",
                        "preferredquality": "192",
                    }
                ],
                "outtmpl": str(filepath_base),
                "quiet": True,
                "no_warnings": True,
                "nocheckcertificate": True,
                "ignoreerrors": True,
                "no_color": True,
                "geo_bypass": True,
                "retries": 5,
                "fragment_retries": 5,
            }

            with youtube_dl.YoutubeDL(ydl_opts) as ydl:
                ydl.download([url])

                # Check if any file was created with the base filename
                new_existing_files = list(domain_dir.glob(f"{base_filename}.*"))
                if new_existing_files:
                    downloaded_file = new_existing_files[0]
                    logger.info(f"  yt-dlp successfully downloaded: {downloaded_file}")
                    return downloaded_file

                logger.info(
                    f"  yt-dlp did not create any files, falling back to direct download"
                )
        except Exception as e:
            logger.info(f"  yt-dlp download failed: {e}")
            logger.info(f"  Falling back to direct download")

        # Second attempt: Try direct download if yt-dlp failed
        try:
            logger.info(f"  Attempting direct download from {url}")
            response = self.session.get(url, stream=True)
            if response.status_code == 200:
                logger.info(f"  Direct download successful, saving to: {filepath_direct}")
                with open(filepath_direct, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                return filepath_direct
            else:
                logger.info(
                    f"  Direct download failed with status code: {response.status_code}"
                )
        except Exception as e:
            logger.info(f"  Error during direct download: {e}")

        # If we get here, both methods failed
        return None

