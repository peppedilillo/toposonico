#!/usr/bin/env python3
"""Apply V1 feature engineering to produce feature-selected training data."""

import argparse
import time
from pathlib import Path

import pandas as pd

from autoencoder.preprocessing import engineer_features
from autoencoder.preprocessing import ENGINEERED_DURCLIPPING_DEFAULT
from autoencoder.preprocessing import ENGINEERED_LABELQSPLIT_DEFAULT
from autoencoder.preprocessing import ENGINEERED_LABELBUCKETS_DEFAULT
from autoencoder.preprocessing import ENGINEERED_YMIN_DEFAULT
from autoencoder.preprocessing import ENGINEERED_GENRETHRESHOLD_DEFAULT

OUTPUT_DIR = Path(__file__).parent.parent / "data/engineered"


def main():
    parser = argparse.ArgumentParser(
        description="Apply V1 feature engineering to training data"
    )
    parser.add_argument("input", help="Input parquet file path")
    parser.add_argument(
        "-o", "--output",
        help="Output path (default: data/engineered/v1_{input_stem}.parquet)",
    )
    parser.add_argument(
        "--year-min",
        type=int,
        default=ENGINEERED_YMIN_DEFAULT,
        help=f"Drop tracks released before this year (default: {ENGINEERED_YMIN_DEFAULT})",
    )
    parser.add_argument(
        "--label-buckets",
        type=int,
        default=ENGINEERED_LABELBUCKETS_DEFAULT,
        help=f"Number of frequency-based label buckets (default: {ENGINEERED_LABELBUCKETS_DEFAULT})",
    )
    parser.add_argument(
        "--label-qsplit",
        type=float,
        default=ENGINEERED_LABELQSPLIT_DEFAULT,
        help=f"Quantile split factor for label bucketing (default: {ENGINEERED_LABELQSPLIT_DEFAULT})",
    )
    parser.add_argument(
        "--duration-clip-quantile",
        type=float,
        default=ENGINEERED_DURCLIPPING_DEFAULT,
        help=f"Upper quantile for duration clipping, 1.0 = no clip (default: {ENGINEERED_DURCLIPPING_DEFAULT})",
    )
    parser.add_argument(
        "--genre-threshold",
        type=int,
        default=ENGINEERED_GENRETHRESHOLD_DEFAULT,
        help=f"Genres appearing <= this many times are replaced with niche_token (default: {ENGINEERED_DURCLIPPING_DEFAULT})",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    if args.output:
        output_path = Path(args.output)
    else:
        output_path = OUTPUT_DIR / f"{input_path.stem}.parquet"

    start = time.time()
    print(f"Reading {input_path}...")
    df = pd.read_parquet(input_path)
    print(f"  {len(df):,} tracks")

    print("Engineering features...")
    print(f"  year_min={args.year_min}")
    print(f"  label_buckets={args.label_buckets}")
    print(f"  label_qsplit={args.label_qsplit}")
    print(f"  duration_clip_quantile={args.duration_clip_quantile}")
    print(f"  genre_threshold={args.genre_threshold}")

    df = engineer_features(
        df,
        year_min=args.year_min,
        label_buckets=args.label_buckets,
        label_qsplit=args.label_qsplit,
        duration_clip_quantile=args.duration_clip_quantile,
        genre_threshold=args.genre_threshold,
    )
    print(f"  {len(df):,} tracks after feature engineering")
    print(f"  {len(df.columns)} columns: {list(df.columns)}")

    print(f"Writing {output_path}...")
    df.to_parquet(output_path, index=False)

    print(f"\nDone in {time.time() - start:.1f}s")


if __name__ == "__main__":
    main()
