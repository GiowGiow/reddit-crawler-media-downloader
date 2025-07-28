from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional

from tqdm import tqdm  # type: ignore
from src.domain.entities import DownloadResult, DownloadStatus, Post
from src.domain.services import SongDownloadService
from .ports import DownloaderPort, PostRepositoryPort

logger = logging.getLogger(__name__)


class DownloadSongsUseCase:
    """Fetch many songs, optionally in parallel."""

    DEFAULT_WORKERS = 1
    DEFAULT_DELAY_IN_SECONDS = 2

    def __init__(self, repository: PostRepositoryPort, downloader: DownloaderPort):
        self._post_repository = repository
        self._service = SongDownloadService(downloader)

    def _process_post(self, post: Post) -> tuple[str, DownloadResult]:
        result = self._service.download_for_post(post)
        return post.id, result

    def execute(
        self,
        flairs: list[str],
        limit: int | None = None,
        workers: int = DEFAULT_WORKERS,
        delay: float = DEFAULT_DELAY_IN_SECONDS,
        only_failed: bool = False,
    ) -> None:
        posts: list[Post] = self._post_repository.list_posts(flairs, only_failed)
        if limit is not None and limit > 0:
            posts = posts[:limit]
        total = len(posts)

        logger.info(
            "Starting downloads: %s posts, %s worker(s), %.1fs delay",
            total,
            workers,
            delay,
        )

        success = 0
        futures = []
        if workers > 1:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = [pool.submit(self._process_post, post) for post in posts]
                for future in tqdm(
                    as_completed(futures), total=total, desc="Downloading"
                ):
                    post_id, result = future.result()
                    if result.status == DownloadStatus.SUCCESS:
                        success += 1
                    self._post_repository.save_result(post_id, result)
                    if delay > 0:
                        time.sleep(delay)
        else:
            for post in tqdm(posts, desc="Downloading"):
                post_id, result = self._process_post(post)
                if result.status == DownloadStatus.SUCCESS:
                    success += 1
                self._post_repository.save_result(post_id, result)
                if delay > 0:
                    time.sleep(delay)

        self._post_repository.commit()
        logger.info(
            "Finished: %s/%s successful (%.1f%%)",
            success,
            total,
            (success / total * 100) if total else 0,
        )
