import argparse
import datetime
import logging
from typing import Tuple

import requests

from reddit_scraper.src.constants import API_URL, DownloadType

logger = logging.getLogger(__name__)


def validate_subreddit_or_username_exists(
    name: str, download_type: DownloadType
) -> Tuple[bool, dict]:
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


def number_to_short_str(num: int) -> str:
    """Convert a number to a short representation (e.g. 1.2k)"""
    if num >= 1_000_000:
        return f"{num/1_000_000:.1f}M"
    elif num >= 1_000:
        return f"{num/1_000:.1f}k"
    else:
        return str(num)


def parse_arguments() -> argparse.Namespace:
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
    return args


def parse_dates(args):
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

    logger.info(
        f"Start date: {datetime.datetime.fromtimestamp(start_timestamp/1000).strftime('%Y-%m-%d')}"
    )
    if end_timestamp:
        logger.info(
            f"End date: {datetime.datetime.fromtimestamp(end_timestamp/1000).strftime('%Y-%m-%d')}"
        )
    else:
        logger.info("End date: Now")

    return start_timestamp, end_timestamp
