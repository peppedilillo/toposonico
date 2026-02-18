#!/usr/bin/env python3
"""Clean raw metadata for autoencoder."""

import argparse
import time
from pathlib import Path

import pandas as pd

from src.autoencoder.preprocessing import drop_or_fill_nans
from src.autoencoder.preprocessing import deduplicate_recordings
from src.autoencoder.preprocessing import cast_types


OUTPUT_DIR = Path(__file__).parent.parent / "metadata/data/clean"


def main():
    parser = argparse.ArgumentParser(description="Clean raw metadata")
    parser.add_argument("input", help="Input parquet file (filename or path)")
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.is_absolute() and not input_path.exists():
        input_path = Path(__file__).parent.parent / "data/raw" / input_path

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / input_path.name

    start = time.time()
    print(f"Reading {input_path}...")
    df = pd.read_parquet(input_path)
    print(f"  {len(df):,} tracks")

    print("Cleaning...")
    initial_count = len(df)

    df = drop_or_fill_nans(df)
    print(f"  {len(df):,} tracks after dropping NaNs (-{initial_count - len(df):,})")

    before_dedup = len(df)
    df = deduplicate_recordings(df)
    print(f"  {len(df):,} tracks after deduplication (-{before_dedup - len(df):,})")

    df = cast_types(df)
    print(f"  {len(df):,} tracks final ({100 * len(df) / initial_count:.1f}% of original)")

    print(f"Writing {output_path}...")
    df.to_parquet(output_path, index=False)

    print(f"\nDone in {time.time() - start:.1f}s")


if __name__ == "__main__":
    main()
