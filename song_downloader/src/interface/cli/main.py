from __future__ import annotations

import argparse
import logging
from pathlib import Path

from src.application.services import DownloadSongsUseCase
from src.infrastructure.downloaders import (
    CompositeDownloader,
    SunoDownloader,
    YtDlpDownloader,
)
from src.infrastructure.repositories import RedditFileRepository

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s – %(message)s",
)
logger = logging.getLogger("song_downloader")


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser("reddit‑song‑downloader 📦")
    p.add_argument("--input", required=True, help="JSONL file with Reddit posts")
    p.add_argument(
        "--output",
        default=None,
        help="Where to save updated JSONL (defaults to input file)",
    )
    p.add_argument(
        "--dataset",
        default="dataset",
        help="Directory where audio files will be stored",
    )
    p.add_argument("--max", type=int, help="Limit number of posts")
    p.add_argument(
        "--flairs",
        nargs="+",
        default=[
            "Song - Audio Upload",
            "Song - Human Written Lyrics",
            "Song",
            "Meme Song",
        ],
    )
    p.add_argument(
        "--force", action="store_true", help="Re‑download even if file already exists"
    )
    p.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of parallel download workers (default: 1)",
    )
    p.add_argument(
        "--only-failed",
        action="store_true",
        help="Only download posts that failed previously",
    )
    p.add_argument(
        "--sleep",
        type=float,
        default=2,
        help="Delay between downloads (default: 2s)",
    )
    return p


def main(raw_args: list[str] | None = None):
    args = build_arg_parser().parse_args(raw_args)

    repo = RedditFileRepository(
        Path(args.input), Path(args.output) if args.output else None
    )

    dataset_root = Path(args.dataset)
    dataset_root.mkdir(exist_ok=True, parents=True)

    downloader = CompositeDownloader(
        [
            SunoDownloader(dataset_root, skip_existing=not args.force),
            YtDlpDownloader(dataset_root, skip_existing=not args.force),
        ]
    )

    use_case = DownloadSongsUseCase(repo, downloader)
    use_case.execute(
        flairs=args.flairs,
        limit=args.max,
        workers=args.workers,
        only_failed=args.only_failed,
    )


if __name__ == "__main__":  # pragma: no cover
    main()
