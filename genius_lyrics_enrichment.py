"""
Genius Lyrics Enrichment

Fetches lyrics for all tracks using the Genius API
and outputs a fully rebuilt dataset with lyrics included.
"""

import os
import time
import random
import logging
import re
from typing import Optional, List

import pandas as pd
import lyricsgenius


# =========================
# Configuration
# =========================

INPUT_FILE = "lyrics_emotion_analysis.xlsx"
OUTPUT_FILE = "lyrics_enriched_tracks.xlsx"

MIN_DELAY = 1.0
MAX_DELAY = 1.5


# =========================
# Logging
# =========================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# =========================
# Authentication
# =========================

def authenticate_genius() -> lyricsgenius.Genius:
    """
    Authenticate using environment variable:
    GENIUS_ACCESS_TOKEN
    """
    token = os.getenv("GENIUS_ACCESS_TOKEN")

    if not token:
        raise EnvironmentError(
            "GENIUS_ACCESS_TOKEN not found. "
            "Set it as an environment variable."
        )

    genius = lyricsgenius.Genius(token)
    genius.remove_section_headers = True
    genius.skip_non_songs = True

    return genius


# =========================
# Cleaning Utilities
# =========================

def clean_track_title(title: str) -> str:
    """Remove extra descriptors for better search results."""
    if not isinstance(title, str):
        return ""

    cut_pos = len(title)
    for sep in ["(", "-"]:
        idx = title.find(sep)
        if idx != -1:
            cut_pos = min(cut_pos, idx)

    return title[:cut_pos].strip() or title.strip()


def clean_artist_name(artist: str) -> str:
    """Simplify artist name for search."""
    if not isinstance(artist, str):
        return ""

    for sep in [",", "&", "feat.", "ft."]:
        if sep in artist:
            artist = artist.split(sep)[0]

    return " ".join(artist.split()).strip()


def clean_lyrics(text: str, track_name: str) -> str:
    """Remove Genius-specific noise and formatting artifacts."""
    if not text:
        return ""

    # Remove intro blocks like "Song Title Lyrics"
    search_term = f"{track_name} Lyrics"
    if search_term in text:
        text = text.split(search_term, 1)[1]

    # Remove embed/footer noise
    text = re.sub(r"\d*Embed$", "", text)

    # Remove section headers [Verse], [Chorus], etc.
    text = re.sub(r"\[.*?\]", "", text)

    return text.strip()


# =========================
# Fetch Layer
# =========================

def fetch_lyrics(
    genius: lyricsgenius.Genius,
    title: str,
    artist: str
) -> Optional[str]:
    """Fetch cleaned lyrics from Genius."""
    try:
        clean_title = clean_track_title(title)
        clean_artist = clean_artist_name(artist)

        if not clean_title:
            return None

        logging.info(f"Fetching: '{clean_title}' by '{clean_artist}'")

        song = genius.search_song(clean_title, clean_artist or None)

        if song and song.lyrics:
            lyrics = clean_lyrics(song.lyrics, clean_title)
            return lyrics if lyrics else None

        return None

    except Exception as e:
        logging.debug(f"Error fetching '{title}' by '{artist}': {e}")
        return None


# =========================
# Main Pipeline
# =========================

def main():

    if not os.path.exists(INPUT_FILE):
        raise FileNotFoundError(f"{INPUT_FILE} not found.")

    logging.info("Loading dataset...")
    df = pd.read_excel(INPUT_FILE)

    if len(df.columns) < 2:
        raise ValueError("Dataset must contain at least track and artist columns.")

    # Identify columns
    track_col = "track_name" if "track_name" in df.columns else df.columns[0]
    artist_col = "artist_names" if "artist_names" in df.columns else df.columns[1]

    logging.info(f"Using columns -> track: '{track_col}', artist: '{artist_col}'")
    logging.info(f"Total rows to process: {len(df):,}")

    genius = authenticate_genius()

    lyrics_list: List[Optional[str]] = []

    for index, row in df.iterrows():
        title = row[track_col]
        artist = row[artist_col]

        lyrics = fetch_lyrics(genius, title, artist)
        lyrics_list.append(lyrics)

        time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

    # Replace or create lyrics column
    df["lyrics"] = lyrics_list

    logging.info("Saving enriched dataset...")
    df.to_excel(OUTPUT_FILE, index=False)

    logging.info("Lyrics enrichment complete.")
    logging.info(f"Output saved to {OUTPUT_FILE}")
    logging.info(f"Total tracks processed: {len(df):,}")


if __name__ == "__main__":
    main()