from __future__ import annotations

import re
from pathlib import Path


def sanitize_filename(filename: str) -> str:
    return re.sub(r"[\\/*?:\"<>|]", "_", filename)


def unify_domain(domain: str) -> str:
    if not domain:
        return "N/A"
    d = domain.lower().strip()
    yt = {"youtube.com", "youtu.be", "m.youtube.com", "music.youtube.com"}
    if d in yt:
        return "youtube.com"
    sc = {"soundcloud.com", "m.soundcloud.com", "on.soundcloud.com"}
    if d in sc:
        return "soundcloud.com"
    if d == "x.com":
        return "twitter.com"
    return d


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def extract_suno_url(selftext: str) -> str | None:
    selftext = selftext.replace(r"\/", "/")
    match = re.search(r"https://suno\.com/[^\s\)\]\}]+", selftext)
    if match:
        return match.group(0)
    return None


def extract_was_deleted(row):
    _meta = row.get("_meta")
    was_deleted = False
    if isinstance(_meta, dict):
        was_deleted = _meta.get("was_deleted_later", False)

    return was_deleted
