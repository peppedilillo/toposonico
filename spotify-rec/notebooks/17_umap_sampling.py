"""
17_umap_sampling.py — Popularity-tiered geometric sampling for UMAP hyperparameter tuning.

Produces ~102k tracks biased toward the sparse/niche periphery of the embedding space,
which is where UMAP n_neighbors / min_dist settings matter most.

Tier scheme (geometric doubling toward lower popularity):
    90-100 → 100    80-90 → 200    70-80 → 400    60-70 → 800
    50-60 → 1,600   40-50 → 3,200  30-40 → 6,400  20-30 → 12,800
    10-20 → 25,600  0-10  → 51,200   Total ≈ 102k

Two additional filters per tier:
  - playlist_count >= PLAYLIST_COUNT_FLOOR  (removes barely-trained embeddings)
  - artist cap: top ARTIST_CAP tracks per artist by popularity (prevents artist domination)

Usage:
    python 17_umap_sampling.py [--dry-run] [--no-umap]

    --dry-run : print tier breakdown without running UMAP
    --no-umap : sample + save metadata but skip UMAP (useful for inspecting the subset)

Outputs (relative to DATA_DIR):
    umap_subset_meta.parquet   — track_rowid, track_name, artist_name, popularity, playlist_count
    umap_subset_coords.parquet — track_rowid, umap_x, umap_y
"""

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DATA_DIR = Path("../../data/playlist")

EMB_PATH    = DATA_DIR / "embeddings_pure_bolt.parquet"
LOOKUP_PATH = DATA_DIR / "track_lookup.parquet"
VOCAB_PATH  = DATA_DIR / "training_vocab.parquet"

OUT_META   = DATA_DIR / "umap_subset_meta.parquet"
OUT_COORDS = DATA_DIR / "umap_subset_coords.parquet"

PLAYLIST_COUNT_FLOOR = 3   # drop tracks seen in < N playlists during training
ARTIST_CAP           = 3   # max tracks per artist per tier
RANDOM_SEED          = 42

# (lo, hi_exclusive, target_n)  — popularity is [lo, hi)
TIERS = [
    (90, 101,   100),
    (80,  90,   200),
    (70,  80,   400),
    (60,  70,   800),
    (50,  60, 1_600),
    (40,  50, 3_200),
    (30,  40, 6_400),
    (20,  30, 12_800),
    (10,  20, 25_600),
    ( 0,  10, 51_200),
]

UMAP_PARAMS = dict(
    n_components=2,
    n_neighbors=15,
    min_dist=0.1,
    metric="euclidean",
    random_state=RANDOM_SEED,
    verbose=True,
)

EMB_COLS = [f"e{i}" for i in range(128)]

# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------

def load_data():
    print("Loading embeddings …")
    embs = pd.read_parquet(EMB_PATH)                      # track_rowid + e0..e127

    print("Loading track lookup …")
    lookup = pd.read_parquet(LOOKUP_PATH,
                             columns=["track_rowid", "track_name",
                                      "track_popularity", "artist_rowid", "artist_name"])

    print("Loading training vocab (playlist_count) …")
    vocab = pd.read_parquet(VOCAB_PATH,
                            columns=["track_rowid", "playlist_count"])

    print("Joining …")
    df = lookup.merge(embs,  on="track_rowid", how="inner")
    df = df.merge(vocab, on="track_rowid", how="left")
    # tracks not in training vocab have playlist_count NaN → treat as 0
    df["playlist_count"] = df["playlist_count"].fillna(0).astype(int)

    print(f"  Total tracks after join: {len(df):,}")
    return df


# ---------------------------------------------------------------------------
# Inspect distribution (printed, not plotted — script context)
# ---------------------------------------------------------------------------

def inspect_tiers(df):
    print("\n--- Playlist-count distribution by popularity tier ---")
    print(f"{'Tier':>10}  {'Available':>10}  {'pc>=3':>8}  "
          f"{'p50':>6}  {'p90':>6}  {'p99':>6}")
    for lo, hi, target in TIERS:
        sub = df[(df["track_popularity"] >= lo) & (df["track_popularity"] < hi)]
        n_total = len(sub)
        sub_q = sub[sub["playlist_count"] >= PLAYLIST_COUNT_FLOOR]
        n_floor = len(sub_q)
        if n_floor == 0:
            print(f"{lo:>3}-{hi-1:<3}       {n_total:>10}  {n_floor:>8}  (empty after floor)")
            continue
        pc = sub_q["playlist_count"]
        print(f"{lo:>3}-{hi-1:<3}       {n_total:>10}  {n_floor:>8}  "
              f"{pc.quantile(.50):>6.0f}  {pc.quantile(.90):>6.0f}  {pc.quantile(.99):>6.0f}")
    print()


# ---------------------------------------------------------------------------
# Sampling
# ---------------------------------------------------------------------------

def sample_tier(df, lo, hi, target_n, rng):
    """Apply floor + artist cap + random sample for one popularity tier."""
    sub = df[(df["track_popularity"] >= lo) & (df["track_popularity"] < hi)].copy()

    # 1. playlist_count floor
    sub = sub[sub["playlist_count"] >= PLAYLIST_COUNT_FLOOR]
    if sub.empty:
        return sub

    # 2. Artist cap: top-ARTIST_CAP tracks per artist by popularity
    sub = (sub
           .sort_values("track_popularity", ascending=False)
           .groupby("artist_rowid", group_keys=False)
           .head(ARTIST_CAP))

    # 3. Random sample down to target
    if len(sub) > target_n:
        sub = sub.sample(n=target_n, random_state=rng)

    return sub


def build_subset(df):
    rng = np.random.default_rng(RANDOM_SEED)
    tiers_out = []
    print("--- Sampling tiers ---")
    print(f"{'Tier':>10}  {'Available→floored→capped':>26}  {'Sampled':>8}")
    for lo, hi, target_n in TIERS:
        sub_all  = df[(df["track_popularity"] >= lo) & (df["track_popularity"] < hi)]
        sub_floor = sub_all[sub_all["playlist_count"] >= PLAYLIST_COUNT_FLOOR]
        sub_capped = (sub_floor
                      .sort_values("track_popularity", ascending=False)
                      .groupby("artist_rowid", group_keys=False)
                      .head(ARTIST_CAP))
        sampled = sub_capped.sample(n=min(target_n, len(sub_capped)),
                                    random_state=int(rng.integers(1 << 31)))
        print(f"{lo:>3}-{hi-1:<3}  "
              f"{len(sub_all):>10} → {len(sub_floor):>8} → {len(sub_capped):>8}  "
              f"{len(sampled):>8}")
        tiers_out.append(sampled)

    df_sub = pd.concat(tiers_out, ignore_index=True)
    print(f"\nTotal subset size: {len(df_sub):,}")
    return df_sub


# ---------------------------------------------------------------------------
# UMAP
# ---------------------------------------------------------------------------

def run_umap(matrix):
    try:
        import umap
    except ImportError:
        raise ImportError("umap-learn not installed — run: pip install umap-learn")

    print(f"\nRunning UMAP on {matrix.shape[0]:,} × {matrix.shape[1]} matrix …")
    reducer = umap.UMAP(**UMAP_PARAMS)
    coords = reducer.fit_transform(matrix)
    return coords


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true",
                        help="Print tier stats only; no sampling or UMAP")
    parser.add_argument("--no-umap", action="store_true",
                        help="Sample + save metadata but skip UMAP")
    args = parser.parse_args()

    df = load_data()
    inspect_tiers(df)

    if args.dry_run:
        print("Dry-run mode — exiting before sampling.")
        return

    df_sub = build_subset(df)

    # Save metadata
    meta_cols = ["track_rowid", "track_name", "artist_name",
                 "track_popularity", "playlist_count"]
    df_sub[meta_cols].to_parquet(OUT_META, index=False)
    print(f"Saved metadata → {OUT_META}")

    if args.no_umap:
        print("--no-umap set — skipping UMAP.")
        return

    matrix = df_sub[EMB_COLS].to_numpy(dtype=np.float32)
    coords = run_umap(matrix)

    coords_df = pd.DataFrame({
        "track_rowid": df_sub["track_rowid"].values,
        "umap_x":      coords[:, 0].astype(np.float32),
        "umap_y":      coords[:, 1].astype(np.float32),
    })
    coords_df.to_parquet(OUT_COORDS, index=False)
    print(f"Saved UMAP coords → {OUT_COORDS}")


if __name__ == "__main__":
    main()
