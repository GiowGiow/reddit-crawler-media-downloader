import enum
import logging
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urljoin

from song_downloader.src.downloader import MusicDownloader

logger = logging.getLogger(__name__)


class DownloadStatus(enum.Enum):
    """Enum for download status results."""

    SUCCESS = "Downloaded successfully"
    SKIPPED_EXISTS = "Skipped: File already exists"
    SKIPPED_NO_URL = "Skipped: No valid URL found"
    FAILED = "Failed: Download was not successful"
    ERROR = "Error during processing"


class PostProcessor:
    """Class to handle processing and downloading of posts."""

    def __init__(self, downloader: MusicDownloader):
        self.downloader = downloader

    def log_post_info(
        self, post_id: str, domain: str, title: str, url: str, reddit_url: str
    ) -> None:
        """Log information about the post being processed."""
        logger.info(f"Processing [{post_id}] - Domain: {domain}")
        logger.info(f"  Title: {title}")
        logger.info(f"  URL: {url}")
        logger.info(f"  Reddit URL: {reddit_url}")

    def process_post(self, row: Dict[str, Any]) -> Tuple[Optional[str], DownloadStatus]:
        """Process a single post and return download results."""
        post_id = row.get("id")
        title = row.get("title", "No title")
        url = row.get("url", "No URL")
        domain = row.get("domain_unified", "Unknown domain")
        permalink = row.get("permalink", None)

        # Construct Reddit URL if permalink exists
        reddit_url = (
            urljoin("https://reddit.com", permalink) if permalink else "No Reddit URL"
        )

        self.log_post_info(post_id, domain, title, url, reddit_url)

        # Check if the URL is valid
        if not url or url == "No URL":
            status = DownloadStatus.SKIPPED_NO_URL
            logger.info(f"  Status: {status.value}")
            return None, status

        download_path = self.downloader.download_by_domain(row)

        # Determine status based on download result
        if download_path:
            if self.downloader.skip_existing and "skipping download" in str(
                download_path
            ):
                status = DownloadStatus.SKIPPED_EXISTS
            else:
                status = DownloadStatus.SUCCESS
            path_str = str(download_path)
        else:
            status = DownloadStatus.FAILED
            path_str = None

        logger.info(f"  Status: {status.value}")
        logger.info("-" * 80)

        return path_str, status

    def log_download_summary(self, total: int, success: int) -> None:
        """Log summary of download operations."""
        failed = total - success

        logger.info("\nDownload Summary:")
        logger.info(f"  Total processed: {total}")
        if total > 0:
            logger.info(f"  Successfully downloaded: {success} ({success/total:.1%})")
            logger.info(f"  Failed: {failed} ({failed/total:.1%})")
        else:
            logger.info("  No items processed")
