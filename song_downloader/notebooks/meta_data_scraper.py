# suno_metadata_scraper.py

import pandas as pd
import asyncio
import os
import json
import time
from datetime import datetime
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from tqdm import tqdm
import concurrent.futures
import random

# Constants
CACHE_DIR = "suno_cache"
CACHE_FILE = os.path.join(CACHE_DIR, "metadata_cache.json")
MAX_CONCURRENT_BROWSERS = 5  # Adjust based on your system capabilities
SONGS_PER_BROWSER = 10
RETRY_ATTEMPTS = 3
REQUEST_DELAY_MIN = 0.5  # Reduced from 1.5 seconds to 0.5 seconds
REQUEST_DELAY_MAX = 1.5  # Reduced from 3.0 seconds to 1.5 seconds

# Ensure cache directory exists
os.makedirs(CACHE_DIR, exist_ok=True)


def load_metadata_cache():
    """Load previously scraped metadata from cache"""
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_metadata_cache(cache):
    """Save metadata cache to file"""
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


async def scrape_suno_song(url, retry_attempt=0):
    """
    Scrapes a Suno.com song page to extract song styles (tags) and lyrics

    Args:
        url (str): The URL of the Suno.com song page
        retry_attempt (int): Current retry attempt number

    Returns:
        dict: Dictionary with 'tags' (list), 'lyrics' (str), and 'instrumental' (bool)
    """
    if (
        not url
        or not isinstance(url, str)
        or not url.startswith("https://suno.com/song/")
    ):
        return {"tags": [], "lyrics": "", "instrumental": False}

    tags = []
    lyrics = ""
    instrumental = False

    # Add random delay to avoid overwhelming the server
    await asyncio.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))

    try:
        async with async_playwright() as p:
            # Launch browser with reduced resource usage
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--disable-gpu",
                    "--disable-dev-shm-usage",
                    "--disable-setuid-sandbox",
                ],
            )

            try:
                # Create a new page with minimal permissions
                context = await browser.new_context(
                    viewport={"width": 1280, "height": 720},
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                )
                page = await context.new_page()

                # Set timeouts to prevent hanging
                page.set_default_timeout(45000)
                page.set_default_navigation_timeout(30000)  # Reduced from 60000

                # Navigate to URL with a faster approach
                print(f"  Navigating to {url}")
                try:
                    # Use 'domcontentloaded' which is much faster than 'networkidle'
                    await page.goto(url, wait_until="domcontentloaded", timeout=30000)

                    # After domcontentloaded, wait a short time for additional content to render
                    await asyncio.sleep(3)
                except Exception as e:
                    # If navigation fails completely, retry or report
                    if "ERR_NAME_NOT_RESOLVED" in str(
                        e
                    ) or "ERR_CONNECTION_REFUSED" in str(e):
                        print(f"  URL not accessible: {e}")
                        if retry_attempt < RETRY_ATTEMPTS:
                            await browser.close()
                            return await scrape_suno_song(url, retry_attempt + 1)
                        return {"tags": [], "lyrics": "", "instrumental": False}
                    else:
                        # For timeout or other issues, we can still try to process what loaded
                        print(f"  Navigation issue, but continuing: {e}")

                # Now get the page content
                html_content = await page.content()
                print(f"  Got page content, length: {len(html_content)} bytes")

                # Parse with BeautifulSoup
                soup = BeautifulSoup(html_content, "html.parser")

                # --- Extract Tags/Styles ---
                print(f"  Looking for style tags...")
                # First try the specific class
                style_container = soup.find(
                    "div",
                    class_="gap-2 font-sans text-secondary/90 text-[12px] whitespace-pre-wrap break-words line-clamp-2",
                )

                # If that fails, try a more general approach
                if not style_container:
                    # Look for any div that might contain style links
                    potential_containers = soup.find_all(
                        "div",
                        class_=lambda c: c
                        and "font-sans" in c
                        and "text-secondary" in c,
                    )
                    if potential_containers:
                        style_container = potential_containers[0]

                if style_container:
                    style_links = style_container.find_all(
                        "a", class_="hover:underline"
                    )
                    for link in style_links:
                        style_name = link.get("title") or link.text
                        if style_name and style_name.strip() not in tags:
                            tags.append(style_name.strip())
                    print(f"  Found {len(tags)} tags")
                else:
                    print(f"  No style tags found")

                # --- Extract Lyrics ---
                print(f"  Looking for lyrics...")
                # Try to find the lyrics container
                lyrics_div = soup.find("div", class_="font-sans text-primary")

                if lyrics_div:
                    print(f"  Found lyrics div")
                    # Found lyrics section, try to extract text
                    text_area = lyrics_div.find("textarea", recursive=False)
                    if text_area:
                        lyrics = text_area.text.strip()
                        print(f"  Extracted lyrics: {len(lyrics)} characters")

                        # Check if lyrics indicate instrumental
                        instrumental = lyrics == "[Instrumental]"
                        if instrumental:
                            print(f"  Song is marked as [Instrumental] in lyrics")
                    else:
                        # If textarea not found but lyrics section exists, check for alternate elements
                        p_tags = lyrics_div.find_all("p")
                        if p_tags:
                            lyrics = p_tags[0].text.strip()
                            print(
                                f"  Extracted lyrics from <p> tag: {len(lyrics)} characters"
                            )
                        else:
                            print(f"  Lyrics div exists but no content found")
                else:
                    # If the specific class isn't found, try a more general approach
                    print(f"  Lyrics div not found by class, trying alternate methods")
                    potential_lyrics_divs = soup.find_all(
                        "div", class_=lambda c: c and "font-sans" in c
                    )

                    if potential_lyrics_divs:
                        for div in potential_lyrics_divs:
                            # Check if it has a textarea
                            text_area = div.find("textarea")
                            if text_area:
                                lyrics = text_area.text.strip()
                                print(
                                    f"  Found lyrics in alternate div: {len(lyrics)} characters"
                                )
                                break

                    if not lyrics:
                        # If still no lyrics, song might be instrumental
                        print(f"  No lyrics found, song may be instrumental")
                        instrumental = True
                        lyrics = "[Instrumental]"

                # Verify the findings
                if not lyrics and not instrumental:
                    print(f"  Warning: No lyrics found and not marked as instrumental")
                    # If we got here with no lyrics but the page seemed to load, it's probably instrumental
                    instrumental = True
                    lyrics = "[Instrumental]"

            except Exception as e:
                if retry_attempt < RETRY_ATTEMPTS:
                    print(
                        f"Error scraping {url}: {e}. Retrying ({retry_attempt+1}/{RETRY_ATTEMPTS})..."
                    )
                    await browser.close()
                    return await scrape_suno_song(url, retry_attempt + 1)
                print(f"Failed to scrape {url} after {RETRY_ATTEMPTS} attempts: {e}")

            finally:
                await browser.close()

    except Exception as e:
        if retry_attempt < RETRY_ATTEMPTS:
            print(
                f"Browser error for {url}: {e}. Retrying ({retry_attempt+1}/{RETRY_ATTEMPTS})..."
            )
            await asyncio.sleep(2)  # Wait before retry
            return await scrape_suno_song(url, retry_attempt + 1)
        print(f"Critical error for {url}: {e}")

    return {"tags": tags, "lyrics": lyrics, "instrumental": instrumental}


async def process_batch(urls, cache):
    """Process a batch of URLs with a single browser instance"""
    results = {}

    for url in urls:
        if url in cache:
            # Use cached result
            results[url] = cache[url]
            # Print that we're using cached result
            tags_str = (
                ", ".join(cache[url]["tags"]) if cache[url]["tags"] else "No tags"
            )
            lyrics_preview = cache[url]["lyrics"][:50].replace("\n", " ") + (
                "..." if len(cache[url]["lyrics"]) > 50 else ""
            )
            print(f"Using cached result for {url}")
            print(f"  Tags: {tags_str}")
            print(f"  Instrumental: {'YES' if cache[url]['instrumental'] else 'NO'}")
            print(f"  Lyrics: {lyrics_preview}")
        else:
            # Need to scrape
            try:
                print(f"Scraping {url}")
                result = await scrape_suno_song(url)
                results[url] = result
                cache[url] = result  # Update cache with new result

                # Print the scraped result
                tags_str = ", ".join(result["tags"]) if result["tags"] else "No tags"
                lyrics_preview = result["lyrics"][:50].replace("\n", " ") + (
                    "..." if len(result["lyrics"]) > 50 else ""
                )
                print(f"  Tags: {tags_str}")
                print(f"  Instrumental: {'YES' if result['instrumental'] else 'NO'}")
                print(f"  Lyrics: {lyrics_preview}")
            except Exception as e:
                print(f"Error processing {url}: {e}")
                results[url] = {"tags": [], "lyrics": "", "instrumental": False}

    return results


async def process_all_songs(urls, cache):
    """Process all songs with controlled concurrency"""
    all_results = {}
    batches = []

    # Create batches of URLs
    for i in range(0, len(urls), SONGS_PER_BROWSER):
        batch = urls[i : i + SONGS_PER_BROWSER]
        batches.append(batch)

    # Create a progress bar for the overall scraping process
    pbar = tqdm(total=len(urls), desc="Scraping songs")

    # Process batches concurrently
    tasks = []
    for i in range(0, len(batches), MAX_CONCURRENT_BROWSERS):
        current_batch = batches[i : i + MAX_CONCURRENT_BROWSERS]
        current_tasks = [process_batch(batch, cache) for batch in current_batch]

        # Wait for the current set of tasks to complete
        batch_results = await asyncio.gather(*current_tasks)

        # Merge results and update progress
        for result_dict in batch_results:
            all_results.update(result_dict)

            # Update progress bar and print details for each song
            for url, metadata in result_dict.items():
                pbar.update(1)

                # Print song details in a compact format
                tags_str = (
                    ", ".join(metadata["tags"]) if metadata["tags"] else "No tags"
                )
                lyrics_preview = metadata["lyrics"][:50].replace("\n", " ") + (
                    "..." if len(metadata["lyrics"]) > 50 else ""
                )
                is_instrumental = "YES" if metadata["instrumental"] else "NO"

                print(f"\n{url}")
                print(f"  Tags: {tags_str}")
                print(f"  Instrumental: {is_instrumental}")
                print(f"  Lyrics: {lyrics_preview}")

        # Save cache after each major batch to prevent data loss
        save_metadata_cache(cache)

        # Print batch completion message
        print(
            f"\n--- Completed batch {i//MAX_CONCURRENT_BROWSERS + 1}/{(len(batches)-1)//MAX_CONCURRENT_BROWSERS + 1} ---\n"
        )

        # Small delay between major batches to be nice to the server
        if i + MAX_CONCURRENT_BROWSERS < len(batches):
            await asyncio.sleep(5)

    pbar.close()
    return all_results


def main():
    """Main function to run the scraping process"""
    print("Loading dataset...")
    # Load your processed dataset
    df = pd.read_csv("suno_ai_processed_dataset.csv")

    # Initialize new columns if they don't exist
    if "lyrics" not in df.columns:
        df["lyrics"] = ""
    if "suno_tags" not in df.columns:
        df["suno_tags"] = None
    if "instrumental" not in df.columns:
        df["instrumental"] = False

    # Load cache
    print("Loading metadata cache...")
    cache = load_metadata_cache()

    # Get list of URLs to process
    urls_to_process = df["suno_url"].dropna().unique().tolist()
    print(f"Found {len(urls_to_process)} unique songs to process")

    # Check which URLs are already in cache
    urls_in_cache = [url for url in urls_to_process if url in cache]
    urls_to_scrape = [url for url in urls_to_process if url not in cache]
    print(f"{len(urls_in_cache)} songs already in cache")
    print(f"{len(urls_to_scrape)} songs need to be scraped")

    if urls_to_scrape:
        print("Starting scraping process...")
        print(
            f"Will scrape in batches of {SONGS_PER_BROWSER} songs with {MAX_CONCURRENT_BROWSERS} concurrent browsers"
        )

        # Run the async function to process all URLs
        start_time = time.time()
        results = asyncio.run(process_all_songs(urls_to_scrape, cache))
        elapsed_time = time.time() - start_time

        # Combine new results with cache
        cache.update(results)

        # Save final cache
        save_metadata_cache(cache)

        print(f"\nScraping completed in {elapsed_time:.2f} seconds")
        print(f"Successfully processed {len(results)} songs")

    # Update the DataFrame with the scraped data
    print("\nUpdating DataFrame with scraped metadata...")

    # Create a progress bar for DataFrame updates
    update_progress = tqdm(df.iterrows(), total=len(df), desc="Updating DataFrame")

    # Track how many records were updated
    updated_count = 0

    for idx, row in update_progress:
        url = row["suno_url"]
        if pd.isna(url) or url not in cache:
            continue

        metadata = cache[url]
        df.at[idx, "lyrics"] = metadata["lyrics"]
        df.at[idx, "suno_tags"] = json.dumps(
            metadata["tags"]
        )  # Store tags as JSON string
        df.at[idx, "instrumental"] = metadata["instrumental"]
        updated_count += 1

    # Save the updated DataFrame with timestamp
    output_file = (
        f'suno_ai_dataset_with_metadata_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    )
    df.to_csv(output_file, index=False)

    print(f"\nDataset with metadata saved to '{output_file}'")
    print(f"Updated {updated_count} out of {len(df)} records")

    # Print summary statistics
    print("\nSummary:")
    print(f"Total songs in dataset: {len(df)}")
    print(f"Songs with lyrics: {(df['lyrics'] != '').sum()}")
    print(f"Songs with tags: {(df['suno_tags'] != '[]').sum()}")
    print(f"Instrumental songs: {df['instrumental'].sum()}")

    # Display sample of the data
    print("\nSample of updated data:")
    sample_df = df[df["lyrics"] != ""].sample(min(5, len(df)))
    for _, row in sample_df.iterrows():
        url = row["suno_url"]
        tags = json.loads(row["suno_tags"]) if not pd.isna(row["suno_tags"]) else []
        tags_str = ", ".join(tags) if tags else "No tags"
        lyrics_preview = row["lyrics"][:50].replace("\n", " ") + (
            "..." if len(row["lyrics"]) > 50 else ""
        )
        print(f"\nTitle: {row['title']}")
        print(f"URL: {url}")
        print(f"Tags: {tags_str}")
        print(f"Instrumental: {'YES' if row['instrumental'] else 'NO'}")
        print(f"Lyrics preview: {lyrics_preview}")


if __name__ == "__main__":
    main()
