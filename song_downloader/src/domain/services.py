"""Pure‑domain behaviours: deciding *what* to do, not *how*."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Protocol

from .constants import AudioDomainType
from .entities import DownloadResult, Post

logger = logging.getLogger(__name__)


class DownloaderPort(Protocol):
    """A service able to fetch a song and return the local file path."""

    def download(
        self, url: str, song_id: str, domain: str
    ) -> Path | None:  # pragma: no cover
        ...


class SongDownloadService:
    """Domain logic for choosing which download strategy to use."""

    def __init__(self, downloader: DownloaderPort):
        self._downloader = downloader

    def download_for_post(self, post: Post) -> DownloadResult:
        logger.info("Processing post \n%s\n", post)
        if not post.url:
            return DownloadResult.skipped("no url")

        # Image posts can be skipped
        if post.hint == "image":
            return DownloadResult.skipped("image post")

        # Had a hosted video but was deleted
        if post.hint == "hosted:video" and post.was_deleted:
            return DownloadResult.skipped("deleted video post")

        if post.is_gallery:
            return DownloadResult.skipped("gallery post")

        # Text post with suno url
        post_url = post.url
        if post.hint == "self" and post.suno_url:
            post_url = post.suno_url

        domain = post.domain

        path = self._downloader.download(post_url, post.id, domain)
        if path is None:
            return DownloadResult.failed("download failed or not audio")
        return DownloadResult.success(path)
