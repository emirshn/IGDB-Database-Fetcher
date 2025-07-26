import pandas as pd
import ast
import sqlite3
from sqlalchemy import create_engine, text

def safe_literal_eval(val):
    """Safely parse stringified Python lists or return None."""
    if not isinstance(val, str):
        return None
    val = val.strip()
    if val == "" or val == "None" or val == "nan" or val == "[]":
        return None
    try:
        return ast.literal_eval(val)
    except Exception:
        return None

# Load CSV
df = pd.read_csv("game_dataset.csv", dtype=str)

# Parse list-like columns
list_cols = [
    "genres", "themes", "franchise", "series",
    "main_developers", "supporting_developers", "publishers",
    "platforms", "player_perspectives", "game_modes", "game_engines",
    "similar_games", "keywords", "screenshot_urls", "artwork_urls"
]
for col in list_cols:
    if col in df.columns:
        df[col] = df[col].apply(safe_literal_eval)

# Convert numeric and date fields
df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
df["aggregated_rating"] = pd.to_numeric(df["aggregated_rating"], errors="coerce")
df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce")

# Clean up text fields
text_cols = ["id", "name", "category", "summary", "cover_url"]
for col in text_cols:
    if col in df.columns:
        df[col] = df[col].astype(str).str.strip().replace({"nan": None})

# Drop duplicates
df.drop_duplicates(subset=["id", "name", "release_date"], inplace=True)

# Connect to SQLite
db_path = "games.db"
engine = create_engine(f"sqlite:///{db_path}")
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Create main table with id as TEXT PRIMARY KEY
cursor.execute("DROP TABLE IF EXISTS games")
cursor.execute("""
    CREATE TABLE games (
        id TEXT PRIMARY KEY,
        name TEXT,
        category TEXT,
        release_date TEXT,
        rating REAL,
        aggregated_rating REAL,
        summary TEXT,
        cover_url TEXT
    )
""")

# Insert games data manually to respect primary key type
main_cols = ["id", "name", "category", "release_date", "rating", "aggregated_rating", "summary", "cover_url"]
df[main_cols].to_sql("games", conn, if_exists="append", index=False)

# Helper to create link tables
def create_link_table(df, list_column, table_name):
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    col_name = list_column[:-1] if list_column.endswith("s") else list_column
    cursor.execute(f"""
        CREATE TABLE {table_name} (
            game_id TEXT,
            {col_name} TEXT
        )
    """)
    rows = []
    for _, row in df.iterrows():
        game_id = row["id"]
        items = row[list_column]
        if isinstance(items, list):
            for item in items:
                rows.append((game_id, item))
    if rows:
        insert_query = f"INSERT INTO {table_name} (game_id, {col_name}) VALUES (?, ?)"
        cursor.executemany(insert_query, rows)

# List-column to table name mapping
many_to_many = {
    "genres": "game_genres",
    "themes": "game_themes",
    "franchise": "game_franchises",
    "series": "game_series",
    "main_developers": "game_main_developers",
    "supporting_developers": "game_supporting_developers",
    "publishers": "game_publishers",
    "platforms": "game_platforms",
    "player_perspectives": "game_perspectives",
    "game_modes": "game_modes",
    "game_engines": "game_engines",
    "similar_games": "game_similar_games",
    "keywords": "game_keywords",
    "screenshot_urls": "game_screenshots",
    "artwork_urls": "game_artworks"
}

# Create all link tables
for col, tbl in many_to_many.items():
    if col in df.columns:
        create_link_table(df, col, tbl)

conn.commit()
conn.close()

print("games.db created with PRIMARY KEY on id and normalized link tables.")
