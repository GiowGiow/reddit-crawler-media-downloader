# Reddit Downloader

# Download both posts and comments
python download_subreddit_data.py sunoai

# Download only posts
python download_subreddit_data.py sunoai --posts

# Download only comments
python download_subreddit_data.py sunoai --comments

# Specify a date range
python download_subreddit_data.py sunoai --start-date 2022-01-01 --end-date 2023-01-01

# Specify an output directory
python download_subreddit_data.py sunoai --output-dir ./sunoai_data
