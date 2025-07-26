# IGDB Fetcher and Game Database Generator

This project contains tools to **fetch**, **clean**, and **normalize** video game data from [IGDB](https://www.igdb.com/) (Internet Game Database), producing a high-quality SQLite database with **350k+ games** for use in recommendation systems, analysis, or game discovery apps.

---

## What's Included

### `fetcher.py`

Fetches raw game data from IGDB's API and saves it as a clean CSV file with enriched metadata:
- Developer, publisher, and studio info
- Genres, themes, engines, modes, and platforms
- Cover images, screenshots, artwork, summaries

### `db_creator.py`

Takes the cleaned CSV from above and turns it into a **normalized SQLite database** with:
- Scalar fields in `games` table
- List fields separated into relational tables (e.g. `game_genres`, `game_platforms`, etc.)

---
## Features
- 350,000+ games with rich metadata
- Retry logic and error handling
- Skips invalid or incomplete games
- Downloads cover/screenshot/artwork URLs
- Resumable fetch with checkpoint
- Caches lookup values to reduce API hits
- Converts timestamps, strings, lists to clean formats
- The rating filter only includes games with average rating > 50 or no rating at all (to avoid shovelware).
- Supports categories: Main games, standalone expansions, remakes, remasters, etc.
- All date fields are normalized to YYYY-MM-DD.
- Field types are suitable for filtering and joining in frontend apps or ML models

## Setup
1. **Install Python dependencies**
2. **Edit these values:**
- CLIENT_ID = "your_client_id"
- CLIENT_SECRET = "your_client_secret"

## Usage
1. Fetch data from IGDB with fetcher.py
2. Generate SQLite database with db_creator.py
