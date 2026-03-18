"""Topology utilities: aggregations and derived representations over track embeddings."""

from typing import Sequence

import pandas as pd


def umap2geo(
    umap_frames: Sequence[tuple[pd.DataFrame, str]],
    max_lon: float = 45.0,
    max_lat: float = 45.0,
    padding: float = 0.02,
) -> list[pd.DataFrame]:
    """Map UMAP coordinates to fake lon/lat using a shared bounding box.

    Computes a single global bbox across all input frames (so entity types stay
    spatially aligned), applies fractional padding, then normalises each frame to
    [-max_lon, +max_lon] × [-max_lat, +max_lat].

    Args:
        umap_frames: sequence of (df, key_col) pairs. Each df must have columns
            ``umap_x`` and ``umap_y`` plus the key column.
        max_lon: half-width in degrees for the x axis (default 45).
        max_lat: half-width in degrees for the y axis (default 45).
        padding: fractional padding added to each side of the global bbox (default 0.02).

    Returns:
        List of DataFrames, one per input, with columns [key_col, lon, lat] (float32).
    """
    import numpy as np

    x_min = min(df.umap_x.min() for df, _ in umap_frames)
    x_max = max(df.umap_x.max() for df, _ in umap_frames)
    y_min = min(df.umap_y.min() for df, _ in umap_frames)
    y_max = max(df.umap_y.max() for df, _ in umap_frames)

    x_pad = (x_max - x_min) * padding
    y_pad = (y_max - y_min) * padding
    x_min -= x_pad
    x_max += x_pad
    y_min -= y_pad
    y_max += y_pad

    results = []
    for df, key_col in umap_frames:
        x_norm = (df.umap_x - x_min) / (x_max - x_min)
        y_norm = (df.umap_y - y_min) / (y_max - y_min)
        results.append(
            pd.DataFrame(
                {
                    key_col: df[key_col],
                    "lon": (x_norm * 2 * max_lon - max_lon).round(6).astype(np.float32),
                    "lat": (y_norm * 2 * max_lat - max_lat).round(6).astype(np.float32),
                }
            )
        )
    return results
