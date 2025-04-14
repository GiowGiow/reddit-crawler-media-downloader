import datetime
import json
import logging
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

import requests

logger = logging.getLogger(__name__)


@dataclass
class DownloadStats:
    total_items: int = 0
    start_time: float = 0
    running_time: float = 0
    current_date: Optional[datetime.datetime] = None
    repeated_error_count: int = 0
    is_paused: bool = False
    is_done: bool = False
    has_error: bool = False

    def format_progress(self) -> str:
        if not self.start_time:
            return "Not started"

        elapsed = time.time() - self.start_time
        if self.total_items == 0:
            return "No items downloaded yet"

        items_per_second = self.total_items / max(1, elapsed)

        progress = f"Downloaded {self.total_items} items"
        if self.current_date:
            progress += f" up to {self.current_date.strftime('%Y-%m-%d %H:%M:%S')}"
        progress += f" ({items_per_second:.2f} items/s)"

        if self.is_paused:
            progress += " [PAUSED]"
        elif self.has_error:
            progress += " [ERROR]"
        elif self.is_done:
            progress += " [DONE]"

        return progress


class ArchiveStream:
    def __init__(
        self,
        url: str,
        start_date: int,
        output_file: str,
        item_type: str,
        append: bool = False,
    ):
        self.url = url
        self.start_date = start_date
        self.current_date = start_date
        self.output_file = output_file
        self.item_type = item_type
        self.append = append
        self.stats = DownloadStats()

    def start(self) -> None:
        """Start the download process"""
        mode = "a" if self.append else "w"
        self.stats.start_time = time.time()
        self.stats.is_paused = False

        try:
            with open(self.output_file, mode) as f:
                self._run(f)
        except KeyboardInterrupt:
            logger.info(f"\n{self.item_type} download interrupted.")
            self.stats.is_paused = True

    def _run(self, file_handle) -> None:
        """Run the download loop"""
        while not self.stats.is_paused and not self.stats.is_done:
            try:
                data = self._fetch_data()
                if not data or len(data) == 0:
                    logger.info(f"\n{self.item_type} download complete!")
                    self.stats.is_done = True
                    break

                # Update stats
                self.stats.total_items += len(data)
                last_item = data[-1]
                new_date = last_item.get("created_utc", 0) * 1000
                if new_date == self.current_date:
                    new_date += 1000
                self.current_date = new_date
                self.stats.current_date = datetime.datetime.fromtimestamp(
                    new_date / 1000
                )

                # Write data to file
                for item in data:
                    file_handle.write(json.dumps(item) + "\n")
                file_handle.flush()

                # Reset error count and clear error flag
                self.stats.repeated_error_count = 0
                if self.stats.has_error:
                    self.stats.has_error = False

                # logger.info progress
                logger.info(f"\r{self.stats.format_progress()}", end="", flush=True)

            except Exception as e:
                logger.info(f"\nError: {str(e)}")
                self.stats.has_error = True
                self.stats.repeated_error_count += 1

                # Implement exponential backoff
                if 0.5 * (self.stats.repeated_error_count**2) > 60:
                    logger.info(
                        f"\nToo many repeated errors. {self.item_type} download aborted."
                    )
                    self.stats.is_done = True
                    break
                else:
                    sleep_time = self.stats.repeated_error_count
                    logger.info(f"Retrying in {sleep_time} seconds...")
                    time.sleep(sleep_time)

    def _fetch_data(self) -> List[Dict]:
        """Fetch data from the API"""
        url = f"{self.url}&limit=auto&sort=asc&after={self.current_date}&meta-app=download-tool-cli"
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(
                f"API returned status code {response.status_code}: {response.text}"
            )

        data = response.json()
        if data.get("error"):
            raise Exception(f"API returned error: {data['error']}")

        return data.get("data", [])


class CombinedArchiveStream:
    def __init__(
        self,
        posts_stream: Optional[ArchiveStream] = None,
        comments_stream: Optional[ArchiveStream] = None,
    ):
        self.posts_stream = posts_stream
        self.comments_stream = comments_stream

    def start(self) -> None:
        """Start both streams sequentially"""
        if self.posts_stream:
            logger.info("\nStarting posts download...")
            self.posts_stream.start()

        if self.comments_stream:
            logger.info("\nStarting comments download...")
            self.comments_stream.start()

        logger.info("\nAll downloads complete!")
