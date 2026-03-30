#!/usr/bin/env python3
"""Build persisted FAISS similarity artifacts for all entity types.

Each entity emits two files under $SICK_SIM_DIR:
    {entity}.index      Serialized FAISS index wrapped in IndexIDMap2
    {entity}.meta.json  Builder/search metadata for later backend loading

The script is intentionally opinionated. It bakes in the production-ready
index family and serving defaults chosen from local benchmarks:
    track  -> RR128,IVF{nlist},RaBitQfs4  with nprobe=64
    album  -> IVFFlat with HNSW quantizer, nprobe=64
    artist -> IVFFlat with HNSW quantizer, nprobe=64
    label  -> IVFFlat, nprobe=64
"""

import argparse
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
import json
import math
import os
from pathlib import Path
import time

import faiss
import numpy as np
import pyarrow.parquet as pq

from src.utils import get_simpaths, read_manifest

SEED = 666
DEFAULT_BATCH_SIZE = 65_536
DEFAULT_TRACK_TRAIN_SIZE = 4_000_000
DEFAULT_NPROBE = 64


@dataclass(frozen=True)
class EntitySpec:
    entity: str
    key_column: str
    metric: str
    normalized: bool
    index_family: str
    nprobe_default: int
    quantizer_ef_search: int | None = None
    hnsw_m: int | None = None
    use_hnsw_quantizer: bool = False
    use_rabitq: bool = False
    train_size_default: int | None = None


ENTITY_SPECS = {
    "track": EntitySpec(
        entity="track",
        key_column="track_rowid",
        metric="inner_product",
        normalized=True,
        index_family="rr_ivf_rabitqfs4",
        nprobe_default=DEFAULT_NPROBE,
        use_rabitq=True,
        train_size_default=DEFAULT_TRACK_TRAIN_SIZE,
    ),
    "album": EntitySpec(
        entity="album",
        key_column="album_rowid",
        metric="inner_product",
        normalized=True,
        index_family="ivf_flat_hnsw_quantizer",
        nprobe_default=DEFAULT_NPROBE,
        quantizer_ef_search=64,
        hnsw_m=32,
        use_hnsw_quantizer=True,
    ),
    "artist": EntitySpec(
        entity="artist",
        key_column="artist_rowid",
        metric="inner_product",
        normalized=True,
        index_family="ivf_flat_hnsw_quantizer",
        nprobe_default=DEFAULT_NPROBE,
        quantizer_ef_search=64,
        hnsw_m=32,
        use_hnsw_quantizer=True,
    ),
    "label": EntitySpec(
        entity="label",
        key_column="label_rowid",
        metric="inner_product",
        normalized=True,
        index_family="ivf_flat",
        nprobe_default=DEFAULT_NPROBE,
    ),
}


def compute_nlist(n_rows: int) -> int:
    """Production heuristic from benchmark work: nlist = int(8 * sqrt(n))."""
    return max(1, int(8 * math.sqrt(n_rows)))


def embedding_columns(schema_names: list[str]) -> list[str]:
    cols = [c for c in schema_names if c.startswith("e")]
    cols.sort(key=lambda c: int(c[1:]))
    if not cols:
        raise ValueError("No embedding columns found (expected e0, e1, ...)")
    return cols


def _rows_for_training(
    total_rows: int, requested: int | None, rng: np.random.Generator
) -> np.ndarray | None:
    if (
            requested is None or
            requested <= 0 or
            requested >= total_rows
    ):
        return None
    return np.sort(rng.choice(total_rows, size=requested, replace=False))


def _normalize_rows(matrix: np.ndarray) -> np.ndarray:
    matrix = np.ascontiguousarray(matrix.astype(np.float32, copy=False))
    faiss.normalize_L2(matrix)
    return matrix


def _arrow_batch_to_embeddings(batch, embed_cols: list[str]) -> np.ndarray:
    frame = batch.select(embed_cols).to_pandas()
    return _normalize_rows(frame.to_numpy(dtype=np.float32, copy=False))


def sample_training_vectors(
    parquet_path: Path,
    key_column: str,
    embed_cols: list[str],
    batch_size: int,
    train_size: int | None,
    seed: int,
) -> tuple[np.ndarray, int]:
    pf = pq.ParquetFile(parquet_path)
    total_rows = pf.metadata.num_rows
    rng = np.random.default_rng(seed)
    selected = _rows_for_training(total_rows, train_size, rng)
    columns = [key_column, *embed_cols]

    if selected is None:
        # use the whole dataset for training
        parts = []
        for batch in pf.iter_batches(batch_size=batch_size, columns=columns):
            parts.append(_arrow_batch_to_embeddings(batch, embed_cols))
        return np.concatenate(parts, axis=0), total_rows

    parts = []
    offset = 0
    cursor = 0
    for batch in pf.iter_batches(batch_size=batch_size, columns=columns):
        batch_len = batch.num_rows
        lo = np.searchsorted(selected, offset, side="left")
        hi = np.searchsorted(selected, offset + batch_len, side="left")
        if hi > lo:
            local_rows = selected[lo:hi] - offset
            matrix = _arrow_batch_to_embeddings(batch, embed_cols)
            parts.append(matrix[local_rows])
            cursor += hi - lo
        offset += batch_len

    if cursor != len(selected):
        raise RuntimeError(
            f"Training sampler missed rows: expected {len(selected)}, got {cursor}"
        )
    return np.concatenate(parts, axis=0), len(selected)


def build_index(dim: int, n_rows: int, train_rows: int, spec: EntitySpec):
    requested_nlist = compute_nlist(n_rows)
    nlist = max(1, min(requested_nlist, n_rows, train_rows))

    if spec.use_rabitq:
        factory = f"RR{dim},IVF{nlist},RaBitQfs4"
        index = faiss.index_factory(dim, factory, faiss.METRIC_INNER_PRODUCT)
        return index, {
            "factory": factory,
            "requested_nlist": requested_nlist,
            "nlist": nlist,
            "rabitq_bits": 4,
            "rotation": f"RR{dim}",
        }

    if spec.use_hnsw_quantizer:
        quantizer = faiss.IndexHNSWFlat(dim, spec.hnsw_m)
        index = faiss.IndexIVFFlat(quantizer, dim, nlist, faiss.METRIC_INNER_PRODUCT)
        index.cp.min_points_per_centroid = 5
        index.quantizer_trains_alone = 2
        return index, {
            "factory": "IndexIVFFlat(IndexHNSWFlat)",
            "requested_nlist": requested_nlist,
            "nlist": nlist,
            "hnsw_m": spec.hnsw_m,
            "cp_min_points_per_centroid": index.cp.min_points_per_centroid,
            "quantizer_trains_alone": index.quantizer_trains_alone,
        }

    quantizer = faiss.IndexFlatIP(dim)
    index = faiss.IndexIVFFlat(quantizer, dim, nlist, faiss.METRIC_INNER_PRODUCT)
    index.cp.min_points_per_centroid = 5
    return index, {
        "factory": "IndexIVFFlat(IndexFlatIP)",
        "requested_nlist": requested_nlist,
        "nlist": nlist,
        "cp_min_points_per_centroid": index.cp.min_points_per_centroid,
    }


def configure_search_params(index, spec: EntitySpec) -> None:
    faiss.ParameterSpace().set_index_parameter(index, "nprobe", spec.nprobe_default)

    if spec.quantizer_ef_search is None:
        return

    inner = index.index if hasattr(index, "index") else index
    ivf = faiss.extract_index_ivf(inner)
    ivf.quantizer.hnsw.efSearch = spec.quantizer_ef_search


def write_metadata(
    meta_path: Path,
    *,
    spec: EntitySpec,
    index_path: Path,
    dim: int,
    ntotal: int,
    train_size_used: int,
    build_config: dict,
) -> None:
    payload = {
        "entity": spec.entity,
        "index_path": str(index_path),
        "key_column": spec.key_column,
        "id_type": "int64",
        "dim": dim,
        "ntotal": ntotal,
        "metric": spec.metric,
        "normalized": spec.normalized,
        "index_family": spec.index_family,
        "nprobe_default": spec.nprobe_default,
        "quantizer_ef_search": spec.quantizer_ef_search,
        "train_size_used": train_size_used,
        "faiss_version": getattr(faiss, "__version__", "unknown"),
        "built_at_utc": datetime.now(UTC).isoformat(),
        "artifact_bytes": index_path.stat().st_size,
        "builder": asdict(spec) | build_config,
    }
    with meta_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
        f.write("\n")


def build_entity_artifact(
    entity: str,
    embedding_path: Path,
    *,
    out_index: Path,
    out_meta: Path,
    batch_size: int,
    train_size_override: int | None,
    overwrite: bool,
) -> None:
    """Build and persist a FAISS similarity index for one entity type.

    Reads the embedding parquet, trains the index family specified in
    ENTITY_SPECS[entity], adds all vectors with their integer rowid keys,
    and writes two files: a serialized FAISS index and a JSON metadata
    sidecar with build configuration and serving defaults.

    Args:
        entity: One of "track", "artist", "album", "label".
        embedding_path: Parquet with key column + e0..e{D-1} float32 columns.
        out_index: Destination path for the serialized FAISS index.
        out_meta: Destination path for the JSON metadata sidecar.
        batch_size: Rows per parquet batch during training sampling and adding.
        train_size_override: Cap on training vectors. None uses the per-entity
            default (4M for tracks, full dataset for others). Pass 0 to force
            full-dataset training regardless of the per-entity default.
        overwrite: If False, raises FileExistsError when artifacts already exist.
    """
    if not overwrite:
        existing = [path for path in (out_index, out_meta) if path.exists()]
        if existing:
            raise FileExistsError(
                f"Refusing to overwrite existing artifacts for {entity}: "
                + ", ".join(str(path) for path in existing)
            )

    spec = ENTITY_SPECS[entity]
    pf = pq.ParquetFile(embedding_path)
    total_rows = pf.metadata.num_rows
    embed_cols = embedding_columns(pf.schema.names)
    dim = len(embed_cols)
    train_size = train_size_override
    # note: passing none for `train_size` directly to `sample_training_vectors`
    # would result in training over the whole dataset.
    if train_size is None:
        train_size = spec.train_size_default

    print(f"\n[{entity}] {total_rows:,} rows, {dim}d")
    print(f"  embedding parquet : {embedding_path}")
    print(f"  artifact index    : {out_index}")
    print(f"  artifact metadata : {out_meta}")

    t0 = time.time()
    train_matrix, train_size_used = sample_training_vectors(
        embedding_path,
        spec.key_column,
        embed_cols,
        batch_size,
        train_size,
        SEED,
    )
    print(f"  training rows     : {train_size_used:,}")

    index, build_config = build_index(dim, total_rows, train_size_used, spec)
    print(f"  index family      : {spec.index_family}")
    print(f"  nlist             : {build_config['nlist']:,}")

    train_t0 = time.time()
    index.train(train_matrix)
    print(f"  trained in        : {time.time() - train_t0:.1f}s")
    del train_matrix

    wrapped = faiss.IndexIDMap2(index)
    configure_search_params(wrapped, spec)

    add_t0 = time.time()
    done = 0
    columns = [spec.key_column, *embed_cols]
    for batch in pf.iter_batches(batch_size=batch_size, columns=columns):
        matrix = _arrow_batch_to_embeddings(batch, embed_cols)
        ids = np.asarray(batch.column(spec.key_column).to_pylist(), dtype=np.int64)
        wrapped.add_with_ids(matrix, ids)

        done += len(ids)
        rate = done / max(time.time() - add_t0, 1e-6)
        print(
            f"  add progress      : {done:>10,} / {total_rows:,}  ({rate:,.0f} rows/s)",
            end="\r",
        )
    print()

    out_index.parent.mkdir(parents=True, exist_ok=True)
    faiss.write_index(wrapped, str(out_index))
    write_metadata(
        out_meta,
        spec=spec,
        index_path=out_index,
        dim=dim,
        ntotal=wrapped.ntotal,
        train_size_used=train_size_used,
        build_config=build_config,
    )

    print(f"  wrote artifacts   : {time.time() - t0:.1f}s total")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build persisted FAISS similarity artifacts from embedding parquets."
    )
    parser.add_argument(
        "--manifest",
        default=os.environ.get("SICK_MANIFEST"),
        help="Path to manifest TOML. Defaults to $SICK_MANIFEST.",
    )
    parser.add_argument(
        "--out-dir",
        default=os.environ.get("SICK_SIM_DIR"),
        help="Output directory for .index and .meta.json artifacts. Defaults to $SICK_SIM_DIR.",
    )
    parser.add_argument(
        "--entities",
        nargs="+",
        choices=sorted(ENTITY_SPECS),
        default=sorted(ENTITY_SPECS),
        help="Subset of entities to build.",
    )
    parser.add_argument(
        "--train-size",
        type=int,
        default=None,
        help="Override training subset size for all entities. Use 0 for full dataset.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Rows per parquet batch while sampling/training/adding (default: {DEFAULT_BATCH_SIZE}).",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=None,
        help="Set FAISS OpenMP thread count before building indexes.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing artifacts.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.manifest is None:
        raise ValueError("--manifest or $SICK_MANIFEST is required")
    if args.out_dir is None:
        raise ValueError("--out-dir or $SICK_SIM_DIR is required")

    if args.threads is not None:
        faiss.omp_set_num_threads(args.threads)

    manifest = read_manifest(args.manifest)

    original_sim_dir = os.environ.get("SICK_SIM_DIR")
    os.environ["SICK_SIM_DIR"] = args.out_dir
    try:
        sim_paths = get_simpaths()
    finally:
        if original_sim_dir is None:
            os.environ.pop("SICK_SIM_DIR", None)
        else:
            os.environ["SICK_SIM_DIR"] = original_sim_dir

    print("Building FAISS similarity artifacts")
    print(f"  manifest   : {args.manifest}")
    print(f"  out dir    : {args.out_dir}")
    print(f"  entities   : {', '.join(args.entities)}")
    print(f"  batch size : {args.batch_size:,}")
    if args.threads is not None:
        print(f"  threads    : {args.threads}")

    total_t0 = time.time()
    for entity in args.entities:
        build_entity_artifact(
            entity,
            manifest["embeddings"][entity],
            out_index=sim_paths["index"][entity],
            out_meta=sim_paths["meta"][entity],
            batch_size=args.batch_size,
            train_size_override=args.train_size,
            overwrite=args.overwrite,
        )

    print(f"\nDone in {time.time() - total_t0:.1f}s")


if __name__ == "__main__":
    main()
