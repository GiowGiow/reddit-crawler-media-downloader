from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import asyncio
import os


async def scrape_suno_song(url):
    """
    Scrapes a Suno.com song page to extract song styles (tags) and lyrics.

    Args:
        url (str): The URL of the Suno.com song page.

    Returns:
        tuple: (styles, lyrics) where styles is a list of tag strings and lyrics is a string.
    """
    print(f"Starting scrape of: {url}")
    styles = []
    lyrics = ""

    async with async_playwright() as p:
        # Launch browser in headless mode
        browser = await p.chromium.launch(headless=True)

        try:
            # Create a new page
            page = await browser.new_page()

            # Navigate to URL with adequate timeout
            print(f"Navigating to {url}")
            await page.goto(url, timeout=60000)

            # Wait for content to load - both selectors we're targeting
            print("Waiting for content to load...")

            # Wait for styles container
            styles_selector = "div.my-2 div.gap-2.font-sans.text-secondary\\/90"
            await page.wait_for_selector(styles_selector, timeout=30000)

            # Wait for lyrics container
            lyrics_selector = "div.font-sans.text-primary"
            await page.wait_for_selector(lyrics_selector, timeout=30000)

            # Give a little more time for dynamic content to fully render
            await asyncio.sleep(2)

            # Get the page content for parsing
            html_content = await page.content()

            # Optional: save HTML for debugging
            with open("suno_rendered_page.html", "w", encoding="utf-8") as f:
                f.write(html_content)

            # Parse with BeautifulSoup
            soup = BeautifulSoup(html_content, "html.parser")
            print("HTML parsed with BeautifulSoup")

            # --- Extract Tags/Styles ---
            print("Extracting tags...")
            # Find the style container div
            container_div_target_class_styles = "gap-2 font-sans text-secondary/90 text-[12px] whitespace-pre-wrap break-words line-clamp-2"
            style_container = soup.find("div", class_=container_div_target_class_styles)

            if style_container:
                # Extract all style links
                style_links = style_container.find_all("a", class_="hover:underline")
                for link in style_links:
                    style_name = link.get("title") or link.text
                    if style_name and style_name.strip() not in styles:
                        styles.append(style_name.strip())
                print(f"Found {len(styles)} tags")
            else:
                print("Style container not found")

            # --- Extract Lyrics using your direct approach ---
            print("Extracting lyrics...")
            lyrics_div = soup.find("div", class_="font-sans text-primary")
            if lyrics_div:
                # Find the textarea directly in this div as you suggested
                text_area = lyrics_div.find("textarea", recursive=False)
                if text_area:
                    lyrics = text_area.text
                    print(f"Found lyrics with length: {len(lyrics)}")
                else:
                    print("Textarea not found in lyrics div")
            else:
                print("Lyrics div not found")

        except Exception as e:
            print(f"Error during scraping: {e}")

        finally:
            # Always close the browser
            await browser.close()

    return styles, lyrics


async def main():
    """
    Main function to run the scraping process.
    """
    # Example URL - replace with the song you want to scrape
    url = "https://suno.com/song/41fdcf6a-b3e0-464f-907a-cd65c03b26aa"

    styles, lyrics = await scrape_suno_song(url)

    # Display results
    print("\n--- Extracted Tags ---")
    if styles:
        for style in styles:
            print(f"- {style}")
    else:
        print("No tags found")

    print("\n--- Extracted Lyrics ---")
    if lyrics and lyrics.strip():
        print(lyrics.strip())
    else:
        print("No lyrics extracted")


if __name__ == "__main__":
    asyncio.run(main())
