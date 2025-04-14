# Reddit Downloader

# Download both posts and comments
python reddit-scraper.py sunoai

# Download only posts
python reddit-scraper.py sunoai --posts

# Download only comments
python reddit-scraper.py sunoai --comments

# Specify a date range
python reddit-scraper.py sunoai --start-date 2022-01-01 --end-date 2023-01-01

# Specify an output directory
python reddit-scraper.py sunoai --output-dir ./sunoai_data
