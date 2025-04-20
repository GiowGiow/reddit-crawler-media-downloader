# Suno Song Downloader

A modular tool for downloading songs from Suno AI and other sources, using Reddit post data.

## Installation

1. Install dependencies:
    ```sh
    pip install -r requirements.txt
    ```

## Usage

Run the downloader from the [song_downloader](http://_vscodecontentref_/2) directory:

```sh
python -m src.interface.cli.main --input <input_jsonl> [options]
```

Example usage:
```sh
python -m src.interface.cli.main --input ../reddit_scraper/reddit_data/r_sunoai_posts.jsonl --output suno_ai_posts_with_downloads.jsonl --workers 3
```