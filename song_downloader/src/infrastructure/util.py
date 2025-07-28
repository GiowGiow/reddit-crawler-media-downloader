from __future__ import annotations

import re
from pathlib import Path


def sanitize_filename(filename: str) -> str:
    return re.sub(r"[\\/*?:\"<>|]", "_", filename)


def unify_domain(domain: str) -> str:
    if not domain:
        return "N/A"
    lowercase_domain = domain.lower().strip()
    youtube_domains = {"youtube.com", "youtu.be", "m.youtube.com", "music.youtube.com"}
    if lowercase_domain in youtube_domains:
        return "youtube.com"
    soundcloud_domains = {"soundcloud.com", "m.soundcloud.com", "on.soundcloud.com"}
    if lowercase_domain in soundcloud_domains:
        return "soundcloud.com"
    if lowercase_domain == "x.com":
        return "twitter.com"
    return lowercase_domain


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


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
