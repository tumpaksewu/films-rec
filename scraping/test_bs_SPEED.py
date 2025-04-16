import asyncio
import aiohttp
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm
from more_itertools import chunked
import random
import asyncio
import ssl
import os

BASE_URL = "https://myshows.me/movie/"
HEADERS = {"User-Agent": "Mozilla/5.0"}
MOVIE_IDS = range(41000, 913622)

EMOJI_TO_LABEL = {
    "😌": "relaxed",
    "🤗": "hugging",
    "🤩": "starstruck",
    "😂": "laughing",
    "🤔": "thinking",
    "😳": "flushed",
    "😬": "grimacing",
    "😒": "unamused",
    "🥰": "loving",
    "🙏": "grateful",
    "😢": "crying",
    "🤯": "mindblown",
    "😰": "anxious",
    "🤐": "silent",
    "🤮": "disgusted",
}

sem = asyncio.Semaphore(30)  # Concurrency limit — adjust as needed


async def fetch(session, url):
    async with sem:
        try:
            async with session.get(url, timeout=10) as response:
                if response.status == 404:
                    return None
                return await response.text()
        except:
            return None
    await asyncio.sleep(random.uniform(0.1, 0.3))


def parse_html(html, url):
    soup = BeautifulSoup(html, "lxml")
    data = {}
    data["url"] = url  # Add URL field

    # Titles
    title_tag = soup.select_one("h1.title__main-text")
    en_title_tag = soup.select_one("div.MovieDetails__original")
    data["title"] = title_tag.text.strip() if title_tag else None
    data["title_en"] = en_title_tag.text.strip() if en_title_tag else None

    # Info rows
    rows = soup.select("td.info-row__title")
    for row in rows:
        key = row.get_text(strip=True)
        val_tag = row.find_next_sibling("td", class_="info-row__value")
        if val_tag:
            data[key] = val_tag.get_text(strip=True)

    # MyShows rating and top counter
    try:
        rating_div = soup.select_one(".ShowRating-value")
        data["myshows_rating"] = float(rating_div.select_one("div").text.strip())
        counter_raw = rating_div.select_one(".Counter").text
        counter_clean = (
            counter_raw.replace("(", "")
            .replace(")", "")
            .replace(" ", "")
            .replace(" ", "")
        )
        data["myshows_top"] = int(counter_clean)
    except:
        data["myshows_rating"] = None
        data["myshows_top"] = None

    # Poster
    poster_tag = soup.select_one("div.movie-poster__picture img")
    data["poster_url"] = poster_tag["src"] if poster_tag else None

    # Reactions
    for button in soup.select("div.Reactions button.ReactionButton"):
        emoji_tag = button.select_one("span.ReactionButton__emoji")
        counter_tag = button.select_one("span.ReactionButton__counter")
        if emoji_tag:
            emoji = emoji_tag.text.strip()
            key = f"reaction_{EMOJI_TO_LABEL.get(emoji, emoji)}"
            value = (
                int(counter_tag.text.strip())
                if counter_tag and counter_tag.text.strip().isdigit()
                else 0
            )
            data[key] = value

    # Ensure all known reactions are present
    for label in EMOJI_TO_LABEL.values():
        key = f"reaction_{label}"
        data.setdefault(key, 0)

    # Actors
    actors = []
    for a in soup.select("div.Characters__list a.Character"):
        name_tag = a.select_one("div.Character__name")
        if name_tag:
            actors.append(name_tag.text.strip())
    data["actors"] = ", ".join(actors)

    # Description
    desc_tag = soup.select_one("div.SlidingTabs__descriptioncontent .HtmlContent")
    data["description"] = desc_tag.get_text(" ", strip=True) if desc_tag else None

    return data


async def process_movie(session, movie_id):
    url = f"{BASE_URL}{movie_id}"
    html = await fetch(session, url)
    if not html:
        return None
    return parse_html(html, url)


async def main():
    CHUNK_SIZE = 5000

    results = []
    checkpoint_every = CHUNK_SIZE
    checkpoint_dir = "checkpoints_41k_plus"
    os.makedirs(checkpoint_dir, exist_ok=True)

    sslcontext = ssl.create_default_context()
    connector = aiohttp.TCPConnector(limit=0, ssl=sslcontext)
    timeout = aiohttp.ClientTimeout(total=None)

    async with aiohttp.ClientSession(
        headers=HEADERS, connector=connector, timeout=timeout
    ) as session:
        for chunk_num, chunk in enumerate(chunked(MOVIE_IDS, CHUNK_SIZE), 1):
            tasks = [process_movie(session, mid) for mid in chunk]
            chunk_results = []
            print(f"\nProcessing chunk {chunk_num}...")
            for f in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
                res = await f
                if res:
                    chunk_results.append(res)
            results.extend(chunk_results)

            df = pd.DataFrame(results)
            df.to_csv(
                f"{checkpoint_dir}/checkpoint_{chunk_num * CHUNK_SIZE}.csv",
                index=False,
                encoding="utf-8-sig",
            )


if __name__ == "__main__":
    asyncio.run(main())
