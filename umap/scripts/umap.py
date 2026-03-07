#!/usr/bin/env python3
"""Compute cuML UMAP projections for a track embedding matrix.

Runs a parameter sweep over n_components, n_neighbors, min_dist, and metric,
writing each projection to a separate parquet file.

Usage:
    python compute.py <emb_path> <out_dir> [options]

Examples:
    # Single run
    python compute.py ../fs-trackrec/embeddings_pure_bolt.parquet ../fs-trackrec \\
        --n-neighbors 50 --min-dist 0.01 --metric cosine

    # Multiple values per parameter (4 × 3 = 12 runs)
    python compute.py ../fs-trackrec/embeddings_pure_bolt.parquet ../fs-trackrec \\
        --n-neighbors 10 50 100 --min-dist 0.01 0.1 0.5 1.0 --metric cosine

    # Full default sweep (2 nc × 3 nn × 4 md × 2 metric = 48 runs)
    # --skip-existing lets you resume if interrupted
    python compute.py ../fs-trackrec/embeddings_pure_bolt.parquet ../fs-trackrec \\
        --skip-existing

    # Also project to 3D alongside 2D
    python compute.py ../fs-trackrec/embeddings_pure_bolt.parquet ../fs-trackrec \\
        --n-components 2 3 --n-neighbors 50 --min-dist 0.1 --metric cosine
"""

import argparse
import itertools
from pathlib import Path
import time

import numpy as np
import pandas as pd
from cuml.manifold import UMAP


N_COMPONENTS_DEFAULT = [2]
N_NEIGHBORS_DEFAULT  = [10, 50, 100]
MIN_DIST_DEFAULT     = [0.01, 0.1, 0.5, 1.0]
METRIC_DEFAULT       = ["cosine", "euclidean"]


def main():
    parser = argparse.ArgumentParser(
        description="Compute cuML UMAP projections for a track embedding matrix",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("emb_path", type=Path, help="Path to embeddings parquet")
    parser.add_argument("out_dir", type=Path, help="Output directory for projections")
    parser.add_argument(
        "--n-components", type=int, nargs="+", default=N_COMPONENTS_DEFAULT,
        metavar="INT", help=f"UMAP output dimensions (default: {N_COMPONENTS_DEFAULT})",
    )
    parser.add_argument(
        "--n-neighbors", type=int, nargs="+", default=N_NEIGHBORS_DEFAULT,
        metavar="INT", help=f"Number of neighbors (default: {N_NEIGHBORS_DEFAULT})",
    )
    parser.add_argument(
        "--min-dist", type=float, nargs="+", default=MIN_DIST_DEFAULT,
        metavar="FLOAT", help=f"Minimum distance (default: {MIN_DIST_DEFAULT})",
    )
    parser.add_argument(
        "--metric", type=str, nargs="+", default=METRIC_DEFAULT,
        metavar="STR", help=f"Distance metric (default: {METRIC_DEFAULT})",
    )
    parser.add_argument(
        "--skip-existing", action="store_true",
        help="Skip runs whose output parquet already exists",
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="Pass verbose=True to cuML UMAP",
    )
    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)

    run_tag = args.emb_path.stem
    if run_tag.startswith("embeddings_"):
        run_tag = run_tag[len("embeddings_"):]

    combos = list(itertools.product(args.n_components, args.n_neighbors, args.min_dist, args.metric))
    nc_vals, nn_vals, md_vals, met_vals = (
        len(args.n_components), len(args.n_neighbors), len(args.min_dist), len(args.metric)
    )

    print(f"Embeddings : {args.emb_path}")
    print(f"Out dir    : {args.out_dir}")
    print(f"Run tag    : {run_tag}")
    print(f"Sweep      : {nc_vals} nc × {nn_vals} nn × {md_vals} md × {met_vals} metric = {len(combos)} runs")
    print()

    t0 = time.time()
    print("Loading embeddings ...")
    embs = pd.read_parquet(args.emb_path)
    emb_cols = [c for c in embs.columns if c.startswith("e") and c[1:].isdigit()]
    matrix = embs[emb_cols].to_numpy(dtype=np.float32)
    print(f"  {len(embs):,} tracks × {len(emb_cols)} dims  ({time.time() - t0:.1f}s)")

    skipped = 0

    for i, (nc, nn, md, metric) in enumerate(combos, 1):
        md_str = str(md).replace(".", "d")
        out_path = args.out_dir / f"umap_{nc}d_{run_tag}_nn{nn}_md{md_str}_{metric}.parquet"

        if args.skip_existing and out_path.exists():
            print(f"[{i}/{len(combos)}] skip  {out_path.name}")
            skipped += 1
            continue

        print(f"\n[{i}/{len(combos)}] nc={nc}  nn={nn}  md={md}  metric={metric}")
        t1 = time.time()
        reducer = UMAP(n_components=nc, n_neighbors=nn, min_dist=md, metric=metric, verbose=args.verbose)
        coords = reducer.fit_transform(matrix)

        coord_cols = {f"umap_{('xyz'[j] if j < 3 else str(j))}": coords[:, j].astype(np.float32)
                      for j in range(nc)}
        pd.DataFrame({"track_rowid": embs["track_rowid"].values, **coord_cols}).to_parquet(
            out_path, index=False
        )
        print(f"  → {out_path}  ({len(coords):,} rows, {time.time() - t1:.1f}s)")

    print(f"\nDone. {len(combos) - skipped} runs completed, {skipped} skipped  (total {time.time() - t0:.0f}s)")


if __name__ == "__main__":
    main()
