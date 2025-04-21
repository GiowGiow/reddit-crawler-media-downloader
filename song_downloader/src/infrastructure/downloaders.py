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


class _BaseDownloader:
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
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            }
        )


class SunoDownloader(_BaseDownloader):
    """Direct download from cdn1.suno.ai/{song_id}.mp3"""

    def __init__(self, root: Path, skip_existing: bool = True):
        super().__init__(root / "suno", skip_existing)

    _UUID_RE = re.compile(
        r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"
    )

    def _extract_song_id(self, url: str) -> Optional[str]:
        parsed = urlparse(url)
        path_parts = parsed.path.strip("/").split("/")
        if len(path_parts) >= 2 and path_parts[0] == "song":
            return path_parts[1]
        m = self._UUID_RE.search(url)
        if m:
            return m.group(1)
        return None

    def download(self, url: str, song_id: str, domain: str) -> Optional[Path]:
        if domain not in AudioDomainType.get_suno_domains():
            return None  # Not my job

        outfile = self.root / f"{song_id}.mp3"
        if outfile.exists() and self.skip_existing:
            logger.debug("Suno: skipping existing %s", outfile)
            return outfile

        song_uuid = self._extract_song_id(url)
        if not song_uuid:
            logger.debug("Suno: could not extract id from %s", url)
            return None

        cdn_url = f"https://cdn1.suno.ai/{song_uuid}.mp3"
        try:
            r = self.session.get(cdn_url, stream=True, timeout=30)
            r.raise_for_status()
            with open(outfile, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
            logger.debug("Suno: downloaded %s", outfile)
            return outfile
        except Exception as e:  # noqa: BLE001
            logger.warning("Suno: failed %s – %s", cdn_url, e)
            return None


class YtDlpDownloader(_BaseDownloader):
    """Generic downloader using yt‑dlp."""

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
        "retries": 10,
        "fragment_retries": 10,
        "retry_sleep": {
            "http": 5,
            "fragment": 5,
        },
    }

    def __init__(self, root: Path, skip_existing: bool = True):
        super().__init__(root / "ytdlp", skip_existing)

    def download(self, url: str, song_id: str, domain: str) -> Optional[Path]:
        # Always attempt; caller decides suitability
        out_base = self.root / sanitize_filename(song_id)
        final_mp3 = Path(f"{out_base}.mp3")
        if final_mp3.exists() and self.skip_existing:
            logger.debug("yt‑dlp: skipping existing %s", final_mp3)
            return final_mp3

        opts = self.YDL_OPTS | {"outtmpl": str(out_base)}
        try:
            with youtube_dl.YoutubeDL(opts) as ydl:
                ydl.download([url])
            return final_mp3 if final_mp3.exists() else None
        except yt_dlp.utils.DownloadError as e:
            logger.debug("yt‑dlp failed for %s – %s", url, e)
            return None


class CompositeDownloader:
    """Tries a list of concrete downloaders in order until one succeeds."""

    def __init__(self, downloaders: List[_BaseDownloader]):
        self._downloaders = downloaders

    def download(self, url: str, song_id: str, domain: str):
        for d in self._downloaders:
            path = d.download(url, song_id, domain)
            if path is not None:
                return path
        return None
