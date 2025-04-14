# Constants
from enum import Enum

API_URL = "https://arctic-shift.photon-reddit.com"


class DownloadType(Enum):
    SUBREDDIT = "subreddit"
    USER = "author"
