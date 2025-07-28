from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from enum import Enum


@dataclass(frozen=True, slots=True)
class Post:
    """A Reddit submission that *might* link to a song."""

    id: str
    title: str
    url: str
    domain: str
    flair: str
    permalink: Optional[str]
    hint: str
    was_deleted: bool = False
    suno_url: Optional[str] = None
    is_gallery: bool = False

    def __str__(self) -> str:
        return f"ID: {self.id}\nTitle: {self.title}\nURL: {self.url}\nDomain: {self.domain}\nFlair: {self.flair}\nPermalink: https://www.reddit.com{self.permalink}\nPost Hint: {self.hint}\nDeleted: {self.was_deleted}\nSuno URL: {self.suno_url}\nGallery: {self.is_gallery}"


class DownloadStatus(Enum):
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    ERROR = "error"


class DownloadReason(Enum):
    NO_URL = "no url"
    IMAGE_POST = "image post"
    DELETED_VIDEO_POST = "deleted video post"
    GALLERY_POST = "gallery post"
    DOWNLOAD_FAILED_OR_NOT_AUDIO = "download failed or not audio"


@dataclass(frozen=True, slots=True)
class DownloadResult:
    """Outcome of attempting to fetch audio for a Post."""

    status: DownloadStatus
    local_path: Path | None = None
    reason: DownloadReason | None = None

    # Factory helpers
    @classmethod
    def success(cls, path: Path) -> "DownloadResult":
        return cls(status=DownloadStatus.SUCCESS, local_path=path)

    @classmethod
    def skipped(cls, reason: DownloadReason) -> "DownloadResult":
        return cls(status=DownloadStatus.SKIPPED, reason=reason)

    @classmethod
    def failed(cls, reason: DownloadReason) -> "DownloadResult":
        return cls(status=DownloadStatus.FAILED, reason=reason)
