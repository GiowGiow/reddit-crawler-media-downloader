from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List

from src.domain.entities import DownloadResult, Post


class PostRepositoryPort(ABC):
    @abstractmethod
    def list_posts(self, flair_filter: List[str]) -> List[Post]: ...

    @abstractmethod
    def save_result(self, post_id: str, result: DownloadResult) -> None: ...

    @abstractmethod
    def commit(self) -> None: ...


class DownloaderPort(ABC):
    @abstractmethod
    def download(self, url: str, song_id: str, domain: str): ...
