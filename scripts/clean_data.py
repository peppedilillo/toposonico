#!/usr/bin/env python3
"""Clean raw training data for autoencoder."""

import argparse
import time
from pathlib import Path

import pandas as pd

from autoencoder.preprocessing import ids_fill_and_drop


OUTPUT_DIR = Path(__file__).parent.parent / "data/clean"


def main():
    parser = argparse.ArgumentParser(description="Clean raw training data")
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
    df = ids_fill_and_drop(df)
    print(f"  {len(df):,} tracks after cleaning")

    print(f"Writing {output_path}...")
    df.to_parquet(output_path, index=False)

    print(f"\nDone in {time.time() - start:.1f}s")


if __name__ == "__main__":
    main()
