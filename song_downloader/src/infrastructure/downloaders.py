from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import List, Optional
from urllib.parse import urlparse

import requests
import yt_dlp as youtube_dl
import yt_dlp.utils
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from src.domain.constants import AudioDomainType
from src.infrastructure.util import ensure_dir, sanitize_filename

logger = logging.getLogger(__name__)


class BaseDownloader:
    DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

    def __init__(self, root: Path, skip_existing: bool = True):
        self.root = root
        self.skip_existing = skip_existing
        ensure_dir(root)

        self.session = requests.Session()
        retries = Retry(
            total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504]
        )
        self.session.mount("http://", HTTPAdapter(max_retries=retries))
        self.session.mount("https://", HTTPAdapter(max_retries=retries))
        self.session.headers.update({"User-Agent": self.DEFAULT_USER_AGENT})


class SunoDownloader(BaseDownloader):
    """Direct download from cdn1.suno.ai/{song_id}.mp3"""

    CDN_URL = "https://cdn1.suno.ai"
    CHUNK_SIZE = 8192

    def __init__(self, root: Path, skip_existing: bool = True):
        super().__init__(root / "suno", skip_existing)

    _UUID_RE = re.compile(
        r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"
    )

    def _extract_song_id(self, url: str) -> Optional[str]:
        """
        Extracts the song UUID from a Suno song URL.

        This method matches any UUID in the URL, including:
            https://app.suno.ai/song/{song_id}/
            or any URL containing a UUID.

        Example:
            url = "https://app.suno.ai/song/123e4567-e89b-12d3-a456-426614174000/"
            returns "123e4567-e89b-12d3-a456-426614174000"

        Returns None if no valid UUID is found.
        """
        match = self._UUID_RE.search(url)
        if match:
            return match.group(1)
        return None

    def download(self, url: str, song_id: str, domain: str) -> Path | None:
        if domain not in AudioDomainType.get_suno_domains():
            return None  # Not my job

        # Check if file exists
        output_filepath = self.root / f"{song_id}.mp3"
        if output_filepath.exists() and self.skip_existing:
            logger.debug("Suno: skipping existing %s", output_filepath)
            return output_filepath

        # Get song uuid from url
        song_uuid = self._extract_song_id(url)
        if not song_uuid:
            logger.debug("Suno: could not extract id from %s", url)
            return None

        # Download
        cdn_song_url = f"{self.CDN_URL}/{song_uuid}.mp3"  # This may be brittle
        try:
            response = self.session.get(cdn_song_url, stream=True, timeout=30)
            response.raise_for_status()
            with open(output_filepath, "wb") as file:
                for chunk in response.iter_content(chunk_size=self.CHUNK_SIZE):
                    file.write(chunk)

            logger.debug("Suno: downloaded %s", output_filepath)
            return output_filepath
        except Exception as e:  # noqa: BLE001
            logger.warning("Suno: failed %s – %s", cdn_song_url, e)
            return None


class YtDlpDownloader(BaseDownloader):
    """Generic downloader using yt‑dlp."""

    # yt-dlp options:
    # "format": best available audio,
    # "postprocessors": extract audio as mp3 at 192kbps,
    # "quiet": suppress output,
    # "no_warnings": suppress warnings,
    # "nocheckcertificate": ignore SSL certificate errors,
    # "ignoreerrors": do not ignore errors (fail on error),
    # "no_color": disable colored output,
    # "geo_bypass": bypass geographic restrictions,
    # "retries": number of retries for download,
    # "fragment_retries": number of retries for fragments,
    # "retry_sleep": sleep time between retries for http and fragments.
    NUMBER_OF_RETRIES = 10
    RETRY_SLEEP_SECONDS = 5
    YDL_OPTS = {
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
        "ignoreerrors": False,
        "no_color": True,
        "geo_bypass": True,
        "retries": NUMBER_OF_RETRIES,
        "fragment_retries": NUMBER_OF_RETRIES,
        "retry_sleep": {
            "http": RETRY_SLEEP_SECONDS,
            "fragment": RETRY_SLEEP_SECONDS,
        },
    }

    def __init__(self, root: Path, skip_existing: bool = True):
        super().__init__(root / "ytdlp", skip_existing)

    def download(self, url: str, song_id: str, domain: str) -> Path | None:
        # Always attempt; caller decides suitability
        sanitized_song_id_path = self.root / sanitize_filename(song_id)

        # Check if file exists
        mp3_file_path = Path(f"{sanitized_song_id_path}.mp3")
        if mp3_file_path.exists() and self.skip_existing:
            logger.debug("yt‑dlp: skipping existing %s", mp3_file_path)
            return mp3_file_path

        # Download
        youtube_dl_config = self.YDL_OPTS | {"outtmpl": str(sanitized_song_id_path)}
        try:
            with youtube_dl.YoutubeDL(youtube_dl_config) as youtube_downloader:
                youtube_downloader.download([url])
            return mp3_file_path if mp3_file_path.exists() else None
        except yt_dlp.utils.DownloadError as e:
            logger.debug("yt‑dlp failed for %s – %s", url, e)
            return None


class CompositeDownloader:
    """Tries a list of concrete downloaders in order until one succeeds."""

    def __init__(self, downloaders: list[BaseDownloader]):
        self._downloaders = downloaders

    def download(self, url: str, song_id: str, domain: str) -> Path | None:
        for downloader in self._downloaders:
            downloaded_song_path = downloader.download(url, song_id, domain)
            if downloaded_song_path is not None:
                return downloaded_song_path
        return None
