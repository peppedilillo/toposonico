#!/usr/bin/env python3
"""Parse OndaRock Pietre Miliari HTML → CSV."""
from pathlib import Path
import pandas as pd
from bs4 import BeautifulSoup

HTML_PATH = Path(__file__).parent.parent.parent / "data" / "ondarock.html"
OUT_PATH  = Path(__file__).parent.parent.parent / "data" / "ondarock_milestones.csv"

soup = BeautifulSoup(HTML_PATH.read_bytes(), "html.parser",
                     from_encoding="iso-8859-1")

rows = []
for tr in soup.select("tr"):
    author = tr.select_one("span.autore")
    title = tr.select_one("span.titolo")
    if not author or not title:
        continue   # skip header / non-album rows

    url_tag   = tr.select_one("td.image_titolo_genere a")
    genre    = tr.select_one("td.genere a")
    year      = tr.select_one("td.anno a")
    label = tr.select_one("td.etichetta a")

    rows.append({
        "artist": author.get_text(strip=True),
        "album":  title.get_text(strip=True),
        "genres": genre.get_text(strip=True)    if genre    else "",
        "year":   year.get_text(strip=True)      if year      else "",
        "label":  label.get_text(strip=True) if label else "",
        "url":    url_tag["href"]                if url_tag   else "",
    })

df = pd.DataFrame(rows)
df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int16")
df.to_csv(OUT_PATH, index=False)
print(f"Saved {len(df)} albums → {OUT_PATH}")
print(df.head(10).to_string())
