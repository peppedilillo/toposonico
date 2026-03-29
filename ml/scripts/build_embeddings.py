#!/usr/bin/env python3
"""Build embedding tables from the enriched training vocab and model checkpoint."""

import argparse
import os
from pathlib import Path
import time

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import torch

from src.entities import Albums
from src.entities import Artists
from src.entities import Labels
from src.entities import Tracks
from src.entities import extract_model_dim
from src.entities import extract_model_embeddings
from src.entities import extract_model_rowids

CHUNK_SIZE_DEFAULT = 500_000


def write_embedding_parquet(
    df: pd.DataFrame, id_col: str, output_path: Path, chunk_size: int
) -> None:
    emb_cols = [c for c in df.columns if c != id_col]
    id_type = pa.int32() if df[id_col].dtype.name == "int32" else pa.int64()
    schema = pa.schema(
        [pa.field(id_col, id_type)] + [pa.field(c, pa.float32()) for c in emb_cols]
    )

    total_rows = len(df)
    written = 0
    started_at = time.time()

    with pq.ParquetWriter(output_path, schema) as writer:
        for lo in range(0, total_rows, chunk_size):
            hi = min(lo + chunk_size, total_rows)
            chunk = df.iloc[lo:hi]
            row = {id_col: pa.array(chunk[id_col].to_numpy(), type=id_type)}
            for c in emb_cols:
                row[c] = pa.array(chunk[c].to_numpy(), type=pa.float32())
            writer.write_table(pa.table(row, schema=schema))
            written += hi - lo
            elapsed = time.time() - started_at
            rate = written / elapsed if elapsed > 0 else 0.0
            print(
                f"  {written:>10,} / {total_rows:,} written  ({rate:,.0f} rows/s)",
                end="\r",
            )
    print()


def write_track_embeddings(
    t1_df: pd.DataFrame, model_dict: dict, output_path: Path, chunk_size: int
) -> int:
    valid_ids = Tracks.valid_ids(t1_df, model_dict)
    rowids = extract_model_rowids(model_dict)
    emb = extract_model_embeddings(model_dict)
    embed_dim = extract_model_dim(model_dict)
    emb_cols = [f"e{i}" for i in range(embed_dim)]

    mask = np.isin(rowids, valid_ids.to_numpy())
    rowids = rowids[mask].astype("int64")
    emb = emb[mask].astype("float32")

    schema = pa.schema(
        [pa.field("track_rowid", pa.int64())]
        + [pa.field(c, pa.float32()) for c in emb_cols]
    )

    total_rows = len(rowids)
    written = 0
    started_at = time.time()

    with pq.ParquetWriter(output_path, schema) as writer:
        for lo in range(0, total_rows, chunk_size):
            hi = min(lo + chunk_size, total_rows)
            row = {"track_rowid": pa.array(rowids[lo:hi], type=pa.int64())}
            chunk_arr = emb[lo:hi]
            for i, c in enumerate(emb_cols):
                row[c] = pa.array(chunk_arr[:, i], type=pa.float32())
            writer.write_table(pa.table(row, schema=schema))
            written += hi - lo
            elapsed = time.time() - started_at
            rate = written / elapsed if elapsed > 0 else 0.0
            print(
                f"  {written:>10,} / {total_rows:,} written  ({rate:,.0f} rows/s)",
                end="\r",
            )
    print()
    return total_rows


def main():
    parser = argparse.ArgumentParser(
        description="Build embedding parquets from t1 vocab and model checkpoint",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("model", type=Path, help="Path to .pt model checkpoint file")
    parser.add_argument(
        "--input",
        default=os.environ.get("SICK_T1_VOCAB"),
        help="Enriched training vocab path. Defaults to `SICK_T1_VOCAB`.",
    )
    parser.add_argument(
        "--track-output",
        default=os.environ.get("SICK_EMBEDDING_TRACK"),
        help="Track embedding output path. Defaults to `SICK_EMBEDDING_TRACK`.",
    )
    parser.add_argument(
        "--artist-output",
        default=os.environ.get("SICK_EMBEDDING_ARTIST"),
        help="Artist embedding output path. Defaults to `SICK_EMBEDDING_ARTIST`.",
    )
    parser.add_argument(
        "--album-output",
        default=os.environ.get("SICK_EMBEDDING_ALBUM"),
        help="Album embedding output path. Defaults to `SICK_EMBEDDING_ALBUM`.",
    )
    parser.add_argument(
        "--label-output",
        default=os.environ.get("SICK_EMBEDDING_LABEL"),
        help="Label embedding output path. Defaults to `SICK_EMBEDDING_LABEL`.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=CHUNK_SIZE_DEFAULT,
        help=f"Rows per parquet row group (default: {CHUNK_SIZE_DEFAULT:,})",
    )
    args = parser.parse_args()

    if args.input is None:
        raise ValueError(
            "No `SICK_T1_VOCAB` environment variable set. "
            "Either run with --input argument or define the environment variable."
        )
    for path, envvar in [
        (args.track_output, "SICK_EMBEDDING_TRACK"),
        (args.artist_output, "SICK_EMBEDDING_ARTIST"),
        (args.album_output, "SICK_EMBEDDING_ALBUM"),
        (args.label_output, "SICK_EMBEDDING_LABEL"),
    ]:
        if path is None:
            raise ValueError(
                f"No `{envvar}` environment variable set. "
                f"Either run with the matching output argument or define the environment variable."
            )

    input_path = Path(args.input)
    model_path = Path(args.model)
    if not input_path.exists():
        raise FileNotFoundError(f"Enriched training vocab not found: {input_path}")
    if not model_path.exists():
        raise FileNotFoundError(f"Model not found: {model_path}")

    output_paths = {
        "track": Path(args.track_output),
        "artist": Path(args.artist_output),
        "album": Path(args.album_output),
        "label": Path(args.label_output),
    }
    for output_path in output_paths.values():
        output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Input      : {input_path}")
    print(f"Model      : {model_path}")
    print(f"Track out  : {output_paths['track']}")
    print(f"Artist out : {output_paths['artist']}")
    print(f"Album out  : {output_paths['album']}")
    print(f"Label out  : {output_paths['label']}")
    print(f"Chunk size : {args.chunk_size:,}")
    print()

    t0 = time.time()
    print("Loading enriched training vocab...")
    t1_df = pd.read_parquet(
        input_path,
        columns=[
            "track_rowid",
            "playlist_count",
            "artist_rowid",
            "album_rowid",
            "label_rowid",
        ],
    )
    t1_df["track_rowid"] = t1_df["track_rowid"].astype("int64")
    t1_df["playlist_count"] = t1_df["playlist_count"].astype("int32")
    t1_df["artist_rowid"] = t1_df["artist_rowid"].astype("int64")
    t1_df["album_rowid"] = t1_df["album_rowid"].astype("int64")
    t1_df["label_rowid"] = t1_df["label_rowid"].astype("Int32")
    print(f"  {len(t1_df):,} rows loaded")

    print("Loading checkpoint...")
    model_dict = torch.load(model_path, map_location="cpu", weights_only=False)
    print(f"  {len(model_dict['vocab']['track_rowid']):,} checkpoint rowids loaded")
    print(f"  embedding dim: {model_dict['hparams']['embed_dim']}")
    print()

    t1 = time.time()
    print("Building track embeddings...")
    n_rows = write_track_embeddings(
        t1_df=t1_df,
        model_dict=model_dict,
        output_path=output_paths["track"],
        chunk_size=args.chunk_size,
    )
    size_mb = output_paths["track"].stat().st_size / 1_048_576
    print(
        f"  {n_rows:,} rows  ->  {output_paths['track']}  ({size_mb:.1f} MB, {time.time() - t1:.1f}s)"
    )

    for name, cls, output_path in [
        ("artist", Artists, output_paths["artist"]),
        ("album", Albums, output_paths["album"]),
        ("label", Labels, output_paths["label"]),
    ]:
        t1 = time.time()
        print(f"Building {name} embeddings...")
        result = cls.embeddings(t1_df, model_dict)
        write_embedding_parquet(
            result,
            id_col=result.columns[0],
            output_path=output_path,
            chunk_size=args.chunk_size,
        )
        size_mb = output_path.stat().st_size / 1_048_576
        print(
            f"  {len(result):,} rows  ->  {output_path}  ({size_mb:.1f} MB, {time.time() - t1:.1f}s)"
        )

    print(f"\nDone in {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
