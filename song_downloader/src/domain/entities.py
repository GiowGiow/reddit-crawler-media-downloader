from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional


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


@dataclass(frozen=True, slots=True)
class DownloadResult:
    """Outcome of attempting to fetch audio for a Post."""

    status: str  # "success" | "skipped" | "failed"
    local_path: Optional[Path] = None
    reason: Optional[str] = None

    # Factory helpers -----------------------------------------------------
    @classmethod
    def success(cls, path: Path) -> "DownloadResult":
        return cls(status="success", local_path=path)

    @classmethod
    def skipped(cls, reason: str) -> "DownloadResult":
        return cls(status="skipped", reason=reason)

    @classmethod
    def failed(cls, reason: str) -> "DownloadResult":
        return cls(status="failed", reason=reason)
