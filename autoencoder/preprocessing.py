from collections import Counter

import pandas as pd


DTYPES = {
    "track_rowid": "int64",
    "artist_rowid": "int64",
    "album_rowid": "int64",
    "track_name": "string",
    "artist_name": "string",
    "album_name": "string",
    "label": "string",
    "_label": "string",
    "release_date": "string",
    "id_isrc": "string",
    "artist_genres": "string",
    "_artist_genres": "string",
    "album_type": "string",
    "release_date_precision": "string",
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
    "mode": "bool",
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


def fill_nans(df: pd.DataFrame, unknown_genre_token: str = "<UNKNOWN_GENRE>", ) -> pd.DataFrame:
    """Clean raw training data: handle missing values and set proper dtypes."""
    df = df.drop(columns=["id_upc"])
    df = df.dropna(subset=["id_isrc"])
    if "artist_genres" in df.columns:
        df["artist_genres"] = df["artist_genres"].fillna(unknown_genre_token)
    assert df.isna().sum().sum() == 0
    return (
        df
        .astype({k: v for k, v in DTYPES.items() if k in df.columns})
        .reset_index(drop=True)
    )


def release_date_to_year_and_season(df: pd.DataFrame) -> pd.DataFrame:
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
    season = month.map(season_map).fillna("unknown")

    # Set season to "unknown" when release_date_precision is "year"
    # (pd.to_datetime defaults year-only dates to Jan 1st, which would be "winter")
    if "release_date_precision" in df.columns:
        season = season.where(df["release_date_precision"] != "year", "unknown")

    return pd.DataFrame({
        "release_year": year,
        "release_season": season,
    })


def map_categorical_by_frequency(
    s: pd.Series, labels: list[str], qsplit: float = 0.2
) -> pd.Series:
    """
    Map a categorical series based on value frequency quantiles.

    Values are bucketed into n groups based on their frequency, where n is the number of labels.
    The most frequent values (above the highest quantile threshold) keep their original names, while
    less frequent values are replaced with the corresponding label.

    Args:
        s: Categorical series to map.
        labels: Labels for each bucket (from least to most frequent).
        qsplit: Quantile split factor. Thresholds are 1 - qsplit^i for i in 1..n.

    Returns:
        Series with infrequent values replaced by bucket labels.
    """
    n = len(labels)
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


def genres_mask_under_threshold(
    s: pd.Series,
    threshold: int,
    separator: str = " | ",
    niche_token: str = "<NICHE_GENRE>",
) -> pd.Series:
    """
    Fill missing genre values and mask infrequent genres.

    Args:
        s: Series containing delimited genre strings (e.g., "rock | pop | indie").
        threshold: Genres appearing <= this many times are replaced with niche_token.
        separator: Delimiter used to split genre strings.
        unknown_token: Token for missing/null values.
        niche_token: Token to replace infrequent genres.

    Returns:
        Series of lists with infrequent genres masked.
    """
    counter = Counter()

    def count(xs: list[str]):
        for x in xs:
            counter[x] += 1
        return xs

    def mask_rare(genres: list[str]) -> list[str]:
        return [g if counter[g] > threshold else niche_token for g in genres]

    return (s
        .apply(lambda x: x.split(separator))
        .apply(count)
        .apply(lambda xs: mask_rare(xs)).apply(str)
    )

ENGINEERED_COLUMNS = [
    # Identifiers/Metadata
    "track_rowid",
    "track_name",
    "artist_name",
    "artist_rowid",
    "album_rowid",
    "album_name",
    "_label",
    "artist_genres",
    "_artist_genres",
    # Categorical
    "album_type",
    "release_season",
    "key",
    "time_signature",
    # Numeric
    "mode",
    "tempo",
    "release_year",
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
ENGINEERED_GENRETHRESHOLD_DEFAULT = 100

def engineer_features(
    df: pd.DataFrame,
    year_min: int = ENGINEERED_YMIN_DEFAULT,
    label_buckets: int = ENGINEERED_LABELBUCKETS_DEFAULT,
    label_qsplit: float = ENGINEERED_LABELQSPLIT_DEFAULT,
    duration_clip_quantile: float = ENGINEERED_DURCLIPPING_DEFAULT,
    genre_threshold: int = ENGINEERED_GENRETHRESHOLD_DEFAULT,
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
        genre_threshold: Genres appearing <= this many times are replaced with niche_token.

    Returns:
        Feature-engineered and cleaned dataframe.
    """
    df = df.copy()

    release_date = pd.to_datetime(df["release_date"], format="mixed", errors="coerce")
    drop_mask = release_date.isna() | (release_date.dt.year < year_min)
    df = df[~drop_mask].reset_index(drop=True)

    release_features = release_date_to_year_and_season(df)
    df["release_year"] = release_features["release_year"]
    df["release_season"] = release_features["release_season"]

    label_names = [f"<{'X' * (label_buckets - i)}S_LABEL>" for i in range(label_buckets)]
    df["_label"] = map_categorical_by_frequency(df["label"], label_names, label_qsplit)

    if duration_clip_quantile < 1.0:
        upper = df["duration_ms"].quantile(duration_clip_quantile)
        df["duration_ms"] = df["duration_ms"].clip(upper=upper)

    df["_artist_genres"] = genres_mask_under_threshold(df["artist_genres"], genre_threshold)
    assert df.isna().sum().sum() == 0
    return (
        df[ENGINEERED_COLUMNS]
        .astype({k: v for k, v in DTYPES.items() if k in ENGINEERED_COLUMNS})
        .reset_index(drop=True)
    )
