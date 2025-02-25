#!/usr/bin/env python3
import argparse
import datetime
import json
import os
import sys
import time
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Tuple

import requests

# Constants
API_URL = "https://arctic-shift.photon-reddit.com"


class DownloadType(Enum):
    SUBREDDIT = "subreddit"
    USER = "author"


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
            print(f"\n{self.item_type} download interrupted.")
            self.stats.is_paused = True

    def _run(self, file_handle) -> None:
        """Run the download loop"""
        while not self.stats.is_paused and not self.stats.is_done:
            try:
                data = self._fetch_data()
                if not data or len(data) == 0:
                    print(f"\n{self.item_type} download complete!")
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

                # Print progress
                print(f"\r{self.stats.format_progress()}", end="", flush=True)

            except Exception as e:
                print(f"\nError: {str(e)}")
                self.stats.has_error = True
                self.stats.repeated_error_count += 1

                # Implement exponential backoff
                if 0.5 * (self.stats.repeated_error_count**2) > 60:
                    print(
                        f"\nToo many repeated errors. {self.item_type} download aborted."
                    )
                    self.stats.is_done = True
                    break
                else:
                    sleep_time = self.stats.repeated_error_count
                    print(f"Retrying in {sleep_time} seconds...")
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
            print("\nStarting posts download...")
            self.posts_stream.start()

        if self.comments_stream:
            print("\nStarting comments download...")
            self.comments_stream.start()

        print("\nAll downloads complete!")


def validate_name(name: str, download_type: DownloadType) -> Tuple[bool, dict]:
    """Validate that the subreddit or user exists and get its info"""
    if len(name) < 2:
        raise ValueError("Name must be at least 2 characters long")

    # Get the earliest date
    response = requests.get(
        f"{API_URL}/api/utils/min?{download_type.value}={name}&meta-app=download-tool-cli"
    )
    if response.status_code != 200:
        raise Exception(f"API returned status code {response.status_code}")

    data = response.json()
    if data.get("error"):
        raise Exception(f"API returned error: {data['error']}")
    if data.get("data") is None:
        raise Exception(f"No {download_type.value} with that name found")

    # Get info about the entity
    if download_type == DownloadType.SUBREDDIT:
        info_url = f"{API_URL}/api/subreddits/search?subreddit={name}&meta-app=download-tool-cli"
    else:
        info_url = (
            f"{API_URL}/api/users/search?author={name}&meta-app=download-tool-cli"
        )

    info_response = requests.get(info_url)
    if info_response.status_code != 200:
        raise Exception(f"API returned status code {info_response.status_code}")

    info_data = info_response.json()
    info = info_data.get("data", [{}])[0]

    # Convert date string to timestamp
    date_timestamp = int(
        datetime.datetime.fromisoformat(data["data"].replace("Z", "+00:00")).timestamp()
        * 1000
    )

    return date_timestamp, info


def number_to_short(num: int) -> str:
    """Convert a number to a short representation (e.g. 1.2k)"""
    if num >= 1_000_000:
        return f"{num/1_000_000:.1f}M"
    elif num >= 1_000:
        return f"{num/1_000:.1f}k"
    else:
        return str(num)


def main():
    parser = argparse.ArgumentParser(
        description="Download Reddit posts and comments from a subreddit or user"
    )

    # Required arguments
    parser.add_argument("name", help="Name of the subreddit or user to download")

    # Optional arguments
    parser.add_argument(
        "--type",
        choices=["subreddit", "user"],
        default="subreddit",
        help="Type of entity to download (default: subreddit)",
    )
    parser.add_argument(
        "--posts", action="store_true", default=False, help="Download posts"
    )
    parser.add_argument(
        "--comments", action="store_true", default=False, help="Download comments"
    )
    parser.add_argument(
        "--output-dir", default="./reddit_data", help="Directory to save output files"
    )
    parser.add_argument(
        "--start-date",
        help="Start date in YYYY-MM-DD format (default: earliest available)",
    )
    parser.add_argument(
        "--end-date", help="End date in YYYY-MM-DD format (default: now)"
    )
    parser.add_argument(
        "--append",
        action="store_true",
        default=False,
        help="Append to existing files instead of overwriting",
    )

    args = parser.parse_args()

    # If neither posts nor comments specified, default to both
    if not args.posts and not args.comments:
        args.posts = True
        args.comments = True

    # Convert type to enum
    download_type = (
        DownloadType.SUBREDDIT if args.type == "subreddit" else DownloadType.USER
    )

    try:
        # Validate the name and get info
        start_timestamp, info = validate_name(args.name, download_type)

        # Display info
        if download_type == DownloadType.SUBREDDIT:
            entity_type = "Subreddit"
            entity_prefix = "r/"
        else:
            entity_type = "User"
            entity_prefix = "u/"

        print(f"Found {entity_type}: {entity_prefix}{args.name}")

        # Show approximate counts if available
        if info and "_meta" in info:
            meta = info["_meta"]
            print(
                f"Approximately {number_to_short(meta.get('num_posts', 0))} posts and "
                f"{number_to_short(meta.get('num_comments', 0))} comments"
            )

        # Parse dates
        if args.start_date:
            start_date = datetime.datetime.strptime(args.start_date, "%Y-%m-%d")
            start_timestamp = int(start_date.timestamp() * 1000)
        else:
            # Use the validated start timestamp but set time to midnight
            start_date = datetime.datetime.fromtimestamp(start_timestamp / 1000)
            start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
            start_timestamp = int(start_date.timestamp() * 1000)

        end_timestamp = None
        if args.end_date:
            end_date = datetime.datetime.strptime(args.end_date, "%Y-%m-%d")
            end_timestamp = int(end_date.timestamp() * 1000)

        print(
            f"Start date: {datetime.datetime.fromtimestamp(start_timestamp/1000).strftime('%Y-%m-%d')}"
        )
        if end_timestamp:
            print(
                f"End date: {datetime.datetime.fromtimestamp(end_timestamp/1000).strftime('%Y-%m-%d')}"
            )
        else:
            print("End date: Now")

        # Create output directory if it doesn't exist
        os.makedirs(args.output_dir, exist_ok=True)

        # Set up file prefixes and URLs
        file_prefix = "r_" if download_type == DownloadType.SUBREDDIT else "u_"
        file_prefix += args.name

        end_date_condition = f"&before={end_timestamp}" if end_timestamp else ""

        # Create streams
        posts_stream = None
        comments_stream = None

        if args.posts:
            posts_file = os.path.join(args.output_dir, f"{file_prefix}_posts.jsonl")
            posts_url = f"{API_URL}/api/posts/search?{download_type.value}={args.name}{end_date_condition}"
            posts_stream = ArchiveStream(
                posts_url, start_timestamp, posts_file, "Posts", args.append
            )

        if args.comments:
            comments_file = os.path.join(
                args.output_dir, f"{file_prefix}_comments.jsonl"
            )
            comments_url = f"{API_URL}/api/comments/search?{download_type.value}={args.name}{end_date_condition}"
            comments_stream = ArchiveStream(
                comments_url, start_timestamp, comments_file, "Comments", args.append
            )

        # Start the downloads
        combined_stream = CombinedArchiveStream(posts_stream, comments_stream)
        combined_stream.start()

    except Exception as e:
        print(f"Error: {str(e)}")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
