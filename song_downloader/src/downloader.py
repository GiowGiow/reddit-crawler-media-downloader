"""
Core downloader class for the Suno Downloader.
"""

import concurrent.futures
import logging
import re
import time
from collections import namedtuple
from pathlib import Path
from typing import Dict, Optional, Union
from urllib.parse import urlparse

import pandas as pd
import requests
import yt_dlp as youtube_dl
import yt_dlp.utils
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util import Retry

from song_downloader.src.constants import AudioDomainType
from song_downloader.src.post_processor import DownloadStatus, PostProcessor

# Define a named tuple for download results
DownloadResult = namedtuple("DownloadResult", ["index", "path", "status"])

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

    def _attempt_suno_download(self, cdn_url: str, filepath: Path) -> Optional[Path]:
        """Helper function to attempt downloading from a Suno CDN URL."""
        try:
            logger.info(f"  Attempting download from: {cdn_url}")
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
                return None
        except requests.exceptions.RequestException as e:
            logger.info(f"  Network error downloading Suno audio {cdn_url}: {e}")
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
        filename = f"{post_id}.mp3"
        filepath = self.dirs["suno"] / filename

        existing = self.check_existing_file(filepath)
        if existing:
            return existing

        song_id = None
        try:
            # Try extracting song ID from path first
            parsed_url = urlparse(url)
            path_parts = parsed_url.path.strip("/").split("/")
            if len(path_parts) >= 2 and path_parts[0] == "song":
                song_id = path_parts[1]
                logger.info(f"  Extracted Suno song ID from path: {song_id}")

            # If not found in path, try regex
            if not song_id:
                match = re.search(
                    r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})",
                    url,
                )
                if match:
                    song_id = match.group(1)
                    logger.info(f"  Extracted Suno song ID via regex: {song_id}")

            if song_id:
                cdn_url = f"https://cdn1.suno.ai/{song_id}.mp3"
                return self._attempt_suno_download(cdn_url, filepath)
            else:
                logger.info(f"  Could not extract Suno song ID from URL: {url}")
                return None

        except Exception as e:  # Catch potential errors during parsing/regex
            logger.info(f"  Error processing Suno URL {url}: {e}")
            return None

    def download_using_yt_dlp(
        self, url: str, post_id: str, domain: str
    ) -> Optional[Path]:
        """
        Download from a generic URL using yt-dlp.

        Args:
            url: URL to download from
            post_id: Post ID for the filename
            domain: Domain for categorization

        Returns:
            Path to the downloaded file or None if failed
        """
        domain_dir = self.dirs.get(domain, self.dirs["others"])
        sanitized_post_id = self.sanitize_filename(post_id)

        # Define the output template for yt-dlp (without extension)
        output_template_base = domain_dir / f"{sanitized_post_id}"
        # Define the expected final path after postprocessing
        final_filepath = Path(f"{output_template_base}.mp3")

        # Check if the final MP3 file already exists
        if final_filepath.exists() and self.skip_existing:
            logger.info(
                f"  Found existing MP3 file: {final_filepath}, skipping download for post {post_id}"
            )
            return final_filepath

        logger.info(f"Attempting yt-dlp download for post {post_id} from {url}")
        try:
            # Configure yt-dlp options
            ydl_opts = self.ydl_opts.copy()
            # Pass the base path without extension to outtmpl
            ydl_opts["outtmpl"] = str(output_template_base)

            with youtube_dl.YoutubeDL(ydl_opts) as ydl:
                ydl.download([url])

                # After download, verify the expected MP3 file was created
                if final_filepath.exists():
                    logger.info(
                        f"  yt-dlp successfully downloaded and converted: {final_filepath}"
                    )
                    return final_filepath
                else:
                    # Check if *any* file was created, maybe postprocessing failed
                    downloaded_files = list(domain_dir.glob(f"{sanitized_post_id}.*"))
                    if downloaded_files:
                        downloaded_file = downloaded_files[0]
                        logger.warning(
                            f"  yt-dlp downloaded but failed postprocessing (expected .mp3): {downloaded_file}"
                        )
                        # Decide whether to return the non-mp3 file or None
                        # return downloaded_file # Uncomment if non-mp3 is acceptable
                        return None  # Return None if only mp3 is desired
                    else:
                        logger.warning(
                            f"  yt-dlp ran but no output file found for post {post_id} matching pattern."
                        )
                        return None

        except yt_dlp.utils.DownloadError as e:
            # More specific error from yt-dlp
            logger.warning(f"  yt-dlp download error for post {post_id} ({url}): {e}")
            return None
        except Exception as e:
            # Catch other potential errors during yt-dlp execution
            logger.error(
                f"  Unexpected error during yt-dlp download for post {post_id} ({url}): {e}",
                exc_info=True,
            )
            return None

        # This return is likely unreachable now but kept for safety
        return None

    def download_by_domain(self, row):
        """
        Select appropriate download method based on domain and download the content.

        Args:
            row: DataFrame row containing post information

        Returns:
            Path to downloaded file or None if download failed
        """
        post_id = row["id"]
        url = row.get("url", "No URL")
        domain = row.get("domain_unified", "Unknown domain")

        # Check if the URL is valid
        if not url or url == "No URL":
            return None

        # Determine appropriate downloader based on domain
        if domain in AudioDomainType.get_suno_domains():
            return self.download_suno_audio(url, post_id)
        else:
            return self.download_using_yt_dlp(url, post_id, domain)


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
