from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import List

import pandas as pd

from src.application.ports import PostRepositoryPort
from src.domain.entities import DownloadResult, Post
from src.infrastructure.util import extract_suno_url, extract_was_deleted, unify_domain

logger = logging.getLogger(__name__)


class RedditFileRepository(PostRepositoryPort):
    """Loads posts from a JSONL exported file and writes results back."""

    def __init__(self, input_file: Path, output_file: Path | None = None):
        self._input_path = input_file
        self._output_path = output_file or input_file  # overwrite unless told otherwise
        self._df = pd.read_json(self._input_path, lines=True)

        # Extend data‑frame columns for results if absent
        for col in ("download_status", "download_path", "download_reason"):
            if col not in self._df.columns:
                self._df[col] = None

    # --------------------- PostRepositoryPort impl -----------------------
    def list_posts(self, flair_filter: List[str]) -> List[Post]:
        subset = self._df[self._df["link_flair_text"].isin(flair_filter)]
        subset = subset.copy()
        subset["domain_unified"] = subset["domain"].apply(unify_domain)

        posts: List[Post] = []
        for _, row in subset.iterrows():
            posts.append(
                Post(
                    id=row.get("id"),
                    title=row.get("title", ""),
                    url=row.get("url", ""),
                    domain=row.get("domain_unified", ""),
                    flair=row.get("link_flair_text", ""),
                    permalink=row.get("permalink"),
                    hint=row.get("post_hint", ""),
                    # When a post was deleted by the user or the mod
                    was_deleted=extract_was_deleted(row),
                    suno_url=extract_suno_url(row.get("selftext", "")),
                    is_gallery=row.get("is_gallery", False) == 1,
                )
            )
        logger.info("Loaded %s candidate posts", len(posts))
        return posts

    def save_result(self, post_id: str, result: DownloadResult) -> None:
        sel = self._df["id"] == post_id
        if result.local_path:
            self._df.loc[sel, "download_path"] = str(result.local_path)
        self._df.loc[sel, "download_status"] = result.status
        if result.reason:
            self._df.loc[sel, "download_reason"] = result.reason

    def commit(self) -> None:
        logger.info("Writing updated JSONL to %s", self._output_path)
        self._df.to_json(self._output_path, orient="records", lines=True)
