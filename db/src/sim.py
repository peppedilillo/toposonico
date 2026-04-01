"""FAISS similarity index building for all entity types.

Each entity gets a different index type based on its scale:
- track : OPQ + IVF/HNSW quantizer + PQ fast-scan
- album : OPQ + IVF/HNSW quantizer + PQ fast-scan
- artist: IVF with HNSW quantizer, flat vectors
- label : IVF with flat quantizer, flat vectors
"""

from dataclasses import dataclass
from pathlib import Path

import faiss
import numpy as np
import pandas as pd

EMBEDDING_DIM = 128
SEED = 666


def ivf_compute_nlist(n: int) -> int:
    return int(np.clip(4 * np.sqrt(n), 256, 16384))


def ivf_train_size(n: int, nlist: int) -> int:
    return min(n, 64 * nlist)


def subsample_training(xb: np.ndarray, max_size: int) -> np.ndarray:
    n = xb.shape[0]
    if n <= max_size:
        return xb
    rng = np.random.default_rng(SEED)
    idx = rng.choice(n, size=max_size, replace=False)
    return np.ascontiguousarray(xb[idx])


@dataclass
class SimIndexSpec:
    """Specification for a single entity's FAISS similarity index."""

    entity: str
    factory_string: str
    filter_index: np.ndarray
    d: int = EMBEDDING_DIM

    @property
    def n(self) -> int:
        return len(self.filter_index)

    @property
    def nlist(self) -> int:
        return ivf_compute_nlist(self.n)


def track_spec(filter_index: np.ndarray) -> SimIndexSpec:
    """Track index: OPQ + IVF/HNSW quantizer + PQ fast-scan."""
    nlist = ivf_compute_nlist(len(filter_index))
    return SimIndexSpec(
        entity="track",
        factory_string=f"OPQ128_128,IVF{nlist}_HNSW32,PQ128x4fsr",
        filter_index=filter_index,
    )


def album_spec(filter_index: np.ndarray) -> SimIndexSpec:
    """Album index: OPQ + IVF/HNSW quantizer + PQ fast-scan."""
    nlist = ivf_compute_nlist(len(filter_index))
    return SimIndexSpec(
        entity="album",
        factory_string=f"OPQ128_128,IVF{nlist}_HNSW32,PQ128x4fsr",
        filter_index=filter_index,
    )


def artist_spec(filter_index: np.ndarray) -> SimIndexSpec:
    """Artist index: IVF with HNSW quantizer, flat vectors (no compression)."""
    nlist = ivf_compute_nlist(len(filter_index))
    return SimIndexSpec(
        entity="artist",
        factory_string=f"IVF{nlist}_HNSW32,Flat",
        filter_index=filter_index,
    )


def label_spec(filter_index: np.ndarray) -> SimIndexSpec:
    """Label index: IVF with flat quantizer, flat vectors."""
    nlist = ivf_compute_nlist(len(filter_index))
    return SimIndexSpec(
        entity="label",
        factory_string=f"IVF{nlist},Flat",
        filter_index=filter_index,
    )


def load_filtered_embeddings(
    embedding_path: Path,
    spec: SimIndexSpec,
    rowid_col: str,
) -> np.ndarray:
    """Load embeddings, filter to spec's index, L2-normalize. Returns (n, d) float32."""
    df = pd.read_parquet(embedding_path)
    df = df[df[rowid_col].isin(spec.filter_index)]
    df = df.set_index(rowid_col).loc[spec.filter_index].reset_index()
    matrix = np.ascontiguousarray(df.filter(regex=r"^e\d+$").values, dtype=np.float32)
    faiss.normalize_L2(matrix)
    return matrix


def train_index(spec: SimIndexSpec, xb: np.ndarray) -> faiss.Index:
    """Build, train, and populate a FAISS index from spec + embedding matrix."""
    index = faiss.index_factory(spec.d, spec.factory_string, faiss.METRIC_INNER_PRODUCT)
    train_n = ivf_train_size(spec.n, spec.nlist)
    xt = subsample_training(xb, train_n)
    index.train(xt)
    index = faiss.IndexIDMap2(index)
    index.add_with_ids(xb, spec.filter_index)
    return index


def save_index(index: faiss.Index, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    faiss.write_index(index, str(path))


def load_index(path: Path) -> faiss.Index:
    return faiss.read_index(str(path))
