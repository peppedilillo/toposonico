import pandas as pd


UNKNOWN_GENRE = "<GENRE_UNKNOWN>"

DTYPES = {
    "track_rowid": "int64",
    "artist_rowid": "int64",
    "album_rowid": "int64",
    "track_name": "string",
    "artist_name": "string",
    "album_name": "string",
    "label": "string",
    "release_date": "string",
    "id_isrc": "string",
    "artist_genres": "string",
    "album_type": "category",
    "release_date_precision": "category",
    "tempo": "float32",
    "danceability": "float32",
    "energy": "float32",
    "loudness": "float32",
    "speechiness": "float32",
    "acousticness": "float32",
    "instrumentalness": "float32",
    "liveness": "float32",
    "valence": "float32",
    "time_signature": "uint8",
    "key": "uint8",
    "mode": "uint8",
    "explicit": "bool",
    "track_popularity": "uint8",
    "artist_popularity": "uint8",
    "album_popularity": "uint8",
    "track_number": "int16",
    "disc_number": "uint8",
    "total_tracks": "int16",
    "artist_followers": "int64",
    "duration_ms": "int32",
}


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Clean raw training data: handle missing values and set proper dtypes."""
    df = df.drop(columns=["id_upc"])
    df = df.dropna(subset=["id_isrc"])

    if "artist_genres" in df.columns:
        df["artist_genres"] = df["artist_genres"].fillna(UNKNOWN_GENRE)

    return (
        df
        .astype({k: v for k, v in DTYPES.items() if k in df.columns})
        .reset_index(drop=True)
    )


def extract_release_features(df: pd.DataFrame) -> pd.DataFrame:
    """Extract release_year and release_season from release_date strings."""
    dt = pd.to_datetime(df["release_date"], format="mixed")

    year = dt.dt.year
    month = dt.dt.month

    season_map = {
        12: "winter", 1: "winter", 2: "winter",
        3: "spring", 4: "spring", 5: "spring",
        6: "summer", 7: "summer", 8: "summer",
        9: "fall", 10: "fall", 11: "fall",
    }
    season = month.map(season_map).fillna("unknown").astype("category")

    return pd.DataFrame({
        "release_year": year,
        "release_season": season,
    })


def map_categorical_by_frequency(
    s: pd.Series, n: int, labels: list[str], qsplit: float = 0.2
) -> pd.Series:
    """
    Map a categorical series based on value frequency quantiles.

    Values are bucketed into n groups based on their frequency. The most frequent
    values (above the highest quantile threshold) keep their original names, while
    less frequent values are replaced with the corresponding label.

    Args:
        s: Categorical series to map.
        n: Number of frequency-based buckets.
        labels: Labels for each bucket (from least to most frequent).
        qsplit: Quantile split factor. Thresholds are 1 - qsplit^i for i in 1..n.

    Returns:
        Series with infrequent values replaced by bucket labels.
    """
    if len(labels) != n:
        raise ValueError(f"labels must have exactly {n} elements, got {len(labels)}")

    counts = s.value_counts()
    thresholds = [counts.quantile(1.0 - qsplit**i) for i in range(1, n + 1)]

    mapping = {}
    for val, cnt in counts.items():
        mapped_val = val
        for i, thresh in enumerate(thresholds):
            if cnt < thresh:
                mapped_val = labels[i]
                break
        mapping[val] = mapped_val

    return s.map(mapping)


ENGINEERED_COLUMNS = [
    # Identifiers/Metadata
    "track_rowid",
    "track_name",
    "artist_name",
    "artist_rowid",
    "album_rowid",
    "album_name",
    # Categorical
    "album_type",
    "_label",
    "release_season",
    # Numeric
    "release_year",
    "time_signature",
    "tempo",
    "key",
    "mode",
    "danceability",
    "energy",
    "loudness",
    "speechiness",
    "acousticness",
    "instrumentalness",
    "liveness",
    "valence",
    "explicit",
    "duration_ms",
]

ENGINEERED_YMIN_DEFAULT = 1955
ENGINEERED_LABELBUCKETS_DEFAULT = 2
ENGINEERED_LABELQSPLIT_DEFAULT = 0.2
ENGINEERED_DURCLIPPING_DEFAULT = 0.9986


def engineer_features(
    df: pd.DataFrame,
    year_min: int = ENGINEERED_YMIN_DEFAULT,
    label_buckets: int = ENGINEERED_LABELBUCKETS_DEFAULT,
    label_qsplit: float = ENGINEERED_LABELQSPLIT_DEFAULT,
    duration_clip_quantile: float = ENGINEERED_DURCLIPPING_DEFAULT,
) -> pd.DataFrame:
    """
    Apply V1 feature engineering: date filtering, release features, label bucketing,
    and column selection.

    Args:
        df: Input DataFrame with raw track data.
        year_min: Drop tracks released before this year.
        label_buckets: Number of frequency-based label buckets.
        label_qsplit: Quantile split factor for label bucketing.
        duration_clip_quantile: Upper quantile for duration clipping (1.0 = no clip).

    Returns:
        DataFrame with V1 feature set (24 columns).
    """
    df = df.copy()

    # Parse release_date and drop bad/old dates
    release_date = pd.to_datetime(df["release_date"], format="mixed", errors="coerce")
    drop_mask = release_date.isna() | (release_date.dt.year < year_min)
    df = df[~drop_mask].reset_index(drop=True)

    # Add release_year and release_season
    release_features = extract_release_features(df)
    df["release_year"] = release_features["release_year"]
    df["release_season"] = release_features["release_season"]

    # Add _label (bucketed)
    label_names = [f"<{'X' * (label_buckets - i)}S_LABEL>" for i in range(label_buckets)]
    df["_label"] = map_categorical_by_frequency(
        df["label"], label_buckets, label_names, label_qsplit
    ).astype("category")

    # Clip duration_ms if requested
    if duration_clip_quantile < 1.0:
        upper = df["duration_ms"].quantile(duration_clip_quantile)
        df["duration_ms"] = df["duration_ms"].clip(upper=upper)

    # Select final V1 columns
    return df[ENGINEERED_COLUMNS].reset_index(drop=True)
