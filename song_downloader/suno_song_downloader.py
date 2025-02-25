#!/usr/bin/env python3
"""
Suno Song Downloader

This script downloads songs from Suno AI from Reddit posts in a JSONL file.
It tries yt-dlp first for compatibility with many sites, then falls back to direct downloads.
"""

import argparse
import json
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from urllib.parse import parse_qs, urlparse

import pandas as pd
import requests
import yt_dlp as youtube_dl
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util import Retry


class SunoDownloader:
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
            "soundcloud": self.output_dir / "soundcloud",
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
            print(f"  Found existing file: {filepath}, skipping download")
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
                    print(f"  No fallback URL found in post data")
                    return None

                print(f"  Downloading directly: {video_url}")
                # For reddit videos, download directly
                response = self.session.get(video_url, stream=True)
                if response.status_code == 200:
                    print(f"  Direct download successful, saving to: {filepath}")
                    with open(filepath, "wb") as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)
                    return filepath
                else:
                    print(
                        f"  Direct download failed with status code: {response.status_code}"
                    )
            else:
                print(f"  No video information found in post data")

        except Exception as e:
            print(f"  Error downloading Reddit video: {e}")

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
            else:
                # If we can't extract the song ID from the URL, try to get it from the URL itself
                match = re.search(
                    r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})",
                    url,
                )
                if match:
                    song_id = match.group(1)
                    cdn_url = f"https://cdn1.suno.ai/{song_id}.mp3"

                    print(f"  Using direct CDN URL (from regex): {cdn_url}")

                    # Download the audio file
                    response = self.session.get(cdn_url, stream=True)
                    if response.status_code == 200:
                        print(f"  Downloading audio to: {filepath}")
                        with open(filepath, "wb") as f:
                            for chunk in response.iter_content(chunk_size=8192):
                                f.write(chunk)
                        return filepath
                    else:
                        print(
                            f"  Failed to download audio: HTTP {response.status_code}"
                        )
                        print(f"  URL attempted: {cdn_url}")
                else:
                    print(f"  Could not extract Suno song ID from URL: {url}")
        except Exception as e:
            print(f"  Error downloading Suno audio {url}: {e}")

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
            print(f"  Found existing file: {existing_file}, skipping download")
            return existing_file

        existing = self.check_existing_file(filepath_direct)
        if existing:
            return existing

        # First attempt: Try yt-dlp as it supports many sites
        print(f"  Trying yt-dlp for {url}...")
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
                    print(f"  yt-dlp successfully downloaded: {downloaded_file}")
                    return downloaded_file

                print(
                    f"  yt-dlp did not create any files, falling back to direct download"
                )
        except Exception as e:
            print(f"  yt-dlp download failed: {e}")
            print(f"  Falling back to direct download")

        # Second attempt: Try direct download if yt-dlp failed
        try:
            print(f"  Attempting direct download from {url}")
            response = self.session.get(url, stream=True)
            if response.status_code == 200:
                print(f"  Direct download successful, saving to: {filepath_direct}")
                with open(filepath_direct, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                return filepath_direct
            else:
                print(
                    f"  Direct download failed with status code: {response.status_code}"
                )
        except Exception as e:
            print(f"  Error during direct download: {e}")

        # If we get here, both methods failed
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
    d = domain.lower().strip()
    # unify youtube
    if d in ["youtube.com", "youtu.be", "m.youtube.com", "music.youtube.com"]:
        return "youtube.com"
    # unify soundcloud
    if d in ["soundcloud.com", "m.soundcloud.com", "on.soundcloud.com"]:
        return "soundcloud.com"
    # unify X/Twitter
    if d == "x.com":
        return "twitter.com"
    # handle empty domains
    if not d:
        return "N/A"
    # for everything else, just return as is
    return d


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
        "spotify.com",
        "open.spotify.com",
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

        print(f"Processing [{post_id}] - Domain: {domain}")
        print(f"  Title: {title}")
        print(f"  URL: {url}")
        print(f"  Reddit URL: {reddit_url}")

        # Check if the URL is valid
        if not url or url == "No URL":
            status = "Skipped: No valid URL found"
            print(f"  Status: {status}")
            df.at[idx, "download_status"] = status
            continue

        # Determine appropriate downloader based on domain
        if domain == "v.redd.it":
            print(f"  Using: Reddit video downloader")
            download_path = downloader.download_reddit_video(row)
        elif domain in ["suno.com", "cdn1.suno.ai"]:
            print(f"  Using: Suno audio downloader")
            download_path = downloader.download_suno_audio(url, post_id)
        else:
            # For all other domains, use the generic downloader which tries yt-dlp first
            print(f"  Using: Generic downloader for {domain}")
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
        print(f"  Status: {status}")
        print("-" * 80)

        # Sleep to avoid rate limiting
        time.sleep(sleep_time)

    # Print summary of downloads
    success = df["download_path"].notna().sum()
    failed = len(potential_audio) - success

    print("\nDownload Summary:")
    print(f"  Total processed: {len(potential_audio)}")
    print(f"  Successfully downloaded: {success} ({success/len(potential_audio):.1%})")
    print(f"  Failed: {failed} ({failed/len(potential_audio):.1%})")

    # Group by status for more detailed summary
    if "download_status" in df.columns:
        status_counts = df["download_status"].value_counts()
        print("\nStatus breakdown:")
        for status, count in status_counts.items():
            print(f"  {status}: {count}")

    return df


def main():
    """Main function."""
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
    parser.add_argument("--save", help="Save the updated dataframe to JSONL file")

    # Parse arguments
    args = parser.parse_args()

    # Print banner
    print("\n==================================================")
    print("           SUNO REDDIT SONG DOWNLOADER            ")
    print("==================================================\n")

    # Load the JSONL file
    print(f"Loading data from {args.input}...")
    input_path = Path(args.input)
    df = pd.read_json(input_path, lines=True)

    # Filter by flairs
    flair_filter = args.flairs
    print(f"Filtering by flairs: {', '.join(flair_filter)}")
    ai_songs = df[df["link_flair_text"].isin(flair_filter)]
    print(f"Found {len(ai_songs)} posts with song flairs")

    # Unify domains
    print("Unifying domains...")
    ai_songs["domain_unified"] = ai_songs["domain"].apply(unify_domain)

    # Display domain counts
    domain_counts = ai_songs["domain_unified"].value_counts()
    print("\nDomain counts:")
    for domain, count in domain_counts.head(10).items():
        print(f"  {domain}: {count}")

    # Download songs
    print("\nDownloading songs...")
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
        print(f"\nUpdated dataframe saved to {output_df_path}")

    print("\nDone!")


if __name__ == "__main__":
    main()
