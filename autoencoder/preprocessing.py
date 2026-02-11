from collections import Counter

import numpy as np
import pandas as pd


DTYPES = {
    "track_rowid": "int64",
    "artist_rowid": "int64",
    "album_rowid": "int64",
    "track_name": "string",
    "artist_name": "string",
    "album_name": "string",
    "label": "string",
    "_label_size": "int8",
    "release_date": "string",
    "id_isrc": "string",
    "artist_genres": "string",
    "_artist_genres": "string",
    "album_type": "string",
    "release_date_precision": "string",
    "_release_year": "int16",
    "_release_yday_cos": "float32",
    "_release_yday_sin": "float32",
    "tempo": "float32",
    "danceability": "float32",
    "energy": "float32",
    "loudness": "float32",
    "_loudness": "float32",
    "speechiness": "float32",
    "acousticness": "float32",
    "instrumentalness": "float32",
    "liveness": "float32",
    "valence": "float32",
    "time_signature": "uint8",
    "_time_signature_is_four": "bool",
    "key": "uint8",
    "_key_cos": "float32",
    "_key_sin": "float32",
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


def drop_or_fill_nans(df: pd.DataFrame, unknown_genre_token: str = "<UNKNOWN_GENRE>", ) -> pd.DataFrame:
    """Clean raw training data: handle missing values and set proper dtypes."""
    df = df.drop(columns=["id_upc"])
    df = df.dropna(subset=["id_isrc"])
    if "artist_genres" in df.columns:
        df["artist_genres"] = df["artist_genres"].fillna(unknown_genre_token)
    assert df.isna().sum().sum() == 0
    return df.reset_index(drop=True)


def deduplicate_recordings(df: pd.DataFrame) -> pd.DataFrame:
    """Deduplicate track based on their id_isrc and rowid/name."""
    # i'm keeping last because it seems this is what spotify actually returns with partial string matching.
    # dunno. maybe not the best solution but let's check it out and see.
    # also. this thing still leaves a lot of duplicates, try looking for a popular historic track.
    # we could implement something more aggressive based on regex matching but it comes with its edgecases
    # and i don't want to lose too many track to them.
    # so we should keep this in mind while training and split the train and valid accordingly to prevent data leakage
    df = df.drop_duplicates("id_isrc", keep="last")
    return df.drop_duplicates(["artist_rowid", "track_name"], keep="last").reset_index(drop=True)


def cast_types(df: pd.DataFrame) -> pd.DataFrame:
    return df.astype({k: v for k, v in DTYPES.items() if k in df.columns})


# Circle of fifths encoding: harmonically related keys are adjacent
_KEY_ANGLE_MAP_MAJOR = {i: (i * 7 % 12) * 2 * np.pi / 12.0 for i in range(12)}
_KEY_ANGLE_MAP_MINOR = {i: ((i + 3) * 7 % 12) * 2 * np.pi / 12.0 for i in range(12)}


def encode_keys(df: pd.DataFrame) -> pd.DataFrame:
    """Encode musical key as cyclical features using circle of fifths.

    Maps keys to positions on the circle of fifths, so harmonically related keys
    (a fifth apart) are closer in the encoded space. Relative major/minor keys
    (e.g., C major and A minor) map to the same angle.
    """
    angles = np.zeros(len(df))
    mask_major = df["mode"] == 1  # Spotify API: 1 = major, 0 = minor
    angles[mask_major] = df.loc[mask_major, "key"].map(_KEY_ANGLE_MAP_MAJOR)
    angles[~mask_major] = df.loc[~mask_major, "key"].map(_KEY_ANGLE_MAP_MINOR)
    return pd.DataFrame({
        "_key_cos": np.cos(angles),
        "_key_sin": np.sin(angles),
    })


def flag_odd_time_signatures(df: pd.DataFrame) -> pd.DataFrame:
    """Flag tracks with 4/4 time signature."""
    return pd.DataFrame({
        "_time_signature_is_four": df["time_signature"] == 4
    })


def extract_release_features(df: pd.DataFrame) -> pd.DataFrame:
    """Extract release_year and cyclical encoding of day-of-year from release_date."""
    dt = pd.to_datetime(df["release_date"], format="mixed")
    year = dt.dt.year
    yday = dt.dt.dayofyear

    days_in_year = np.where(dt.dt.is_leap_year, 366, 365)
    yday_angle = 2 * np.pi * yday / days_in_year
    yday_cos = np.cos(yday_angle)
    yday_sin = np.sin(yday_angle)

    # Set to (0, 0) when precision is "year" - equidistant from all points on the unit circle
    mask = df["release_date_precision"] == "year"
    yday_cos[mask] = 0.0
    yday_sin[mask] = 0.0

    return pd.DataFrame({
        "_release_year": year,
        "_release_yday_cos": yday_cos,
        "_release_yday_sin": yday_sin,
    })


def normalize_and_clip_loudness(
        df: pd.DataFrame,
        clip_quantile: float = 1 / 15787 / 2,  # sigma
) -> pd.Series:
    """Normalizes loudness per year and clips under 4-sigma deviations."""
    ln = (
        (df["loudness"] - df.groupby("_release_year")["loudness"].transform("mean")) /
        df.groupby("_release_year")["loudness"].transform("std")
    )
    return ln.clip(lower=ln.quantile(clip_quantile))


def label_to_year_size(df: pd.DataFrame, bucket_num: int, qsplit: float = 0.2) -> pd.Series:
    """
    Map a label to its size at track release time.
    Sizes are organized in 80-20 exponential buckets.
    Label sizes are labelled from 0 to bucket_num - 1, smallest to largest.

    Args:
        df: DataFrame with 'release_date' and 'label' columns.
        bucket_num: Number of size buckets (0 = smallest, bucket_num - 1 = largest).
        qsplit: Quantile split factor. Thresholds are 1 - qsplit^i for i in 1..bucket_num.

    Returns:
        Series of integer bucket labels, aligned to df.index.
    """
    years = pd.to_datetime(df["release_date"], format="mixed").dt.year
    counts = df.groupby([years, "label"]).size().groupby("label").cumsum().sort_index()
    result = pd.Series(pd.NA, index=df.index)
    for year in range(years.min(), years.max() + 1):
        if year not in counts.index.get_level_values(0):
            continue
        snapshot = counts.loc[:year].groupby("label").last()
        thresholds = [
                         snapshot.quantile(1.0 - qsplit ** i)
                         for i in range(1, bucket_num)
                     ] + [snapshot.max() + 1]

        mapping = {}
        for val, cnt in snapshot.items():
            for i, thresh in enumerate(thresholds):
                if cnt < thresh:
                    mapping[val] = i
                    break
        mask = years == year
        result[mask] = df.loc[mask, "label"].map(mapping)
    return result


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


# leading underscores for engineered features
ENGINEERED_COLUMNS = [
    # Identifiers/Metadata
    "track_rowid",
    "track_name",
    "artist_name",
    "artist_rowid",
    "album_rowid",
    "album_name",
    "artist_genres",
    "_artist_genres",
    # Categorical
    "album_type",
    "_label_size",
    # Numeric
    "_time_signature_is_four",
    "_key_cos",
    "_key_sin",
    "mode",
    "tempo",
    "_release_year",
    "_release_yday_cos",
    "_release_yday_sin",
    "danceability",
    "energy",
    "_loudness",
    "speechiness",
    "acousticness",
    "instrumentalness",
    "liveness",
    "valence",
    "explicit",
    "duration_ms",
]


ENGINEERED_YMIN_DEFAULT = 1955
ENGINEERED_LABELSIZEBUCKETS_DEFAULT = 7
ENGINEERED_LABELSIZEQSPLIT_DEFAULT = 0.2
ENGINEERED_DURCLIPPING_DEFAULT = 0.9986
ENGINEERED_GENRETHRESHOLD_DEFAULT = 100


def engineer_features(
    df: pd.DataFrame,
    year_min: int = ENGINEERED_YMIN_DEFAULT,
    label_size_buckets: int = ENGINEERED_LABELSIZEBUCKETS_DEFAULT,
    label_size_qsplit: float = ENGINEERED_LABELSIZEQSPLIT_DEFAULT,
    duration_clip_quantile: float = ENGINEERED_DURCLIPPING_DEFAULT,
    genre_threshold: int = ENGINEERED_GENRETHRESHOLD_DEFAULT,
) -> pd.DataFrame:
    """
    Apply V1 feature engineering: date filtering, release features, label bucketing,
    and column selection.

    Args:
        df: Input DataFrame with raw track data.
        year_min: Drop tracks released before this year.
        label_size_buckets: Number of frequency-based label size buckets.
        label_size_qsplit: Quantile split factor for label size bucketing.
        duration_clip_quantile: Upper quantile for duration clipping (1.0 = no clip).
        genre_threshold: Genres appearing <= this many times are replaced with niche_token.

    Returns:
        Feature-engineered and cleaned dataframe.
    """
    df = df.copy()

    release_date = pd.to_datetime(df["release_date"], format="mixed", errors="coerce")
    # a number of track have invalid release date (e.g. `0000`), we drop them
    drop_mask = release_date.isna() | (release_date.dt.year < year_min)
    df = df[~drop_mask].reset_index(drop=True)

    release_features = extract_release_features(df)
    df["_release_year"] = release_features["_release_year"]
    df["_release_yday_cos"] = release_features["_release_yday_cos"]
    df["_release_yday_sin"] = release_features["_release_yday_sin"]

    key_features = encode_keys(df)
    df["_key_cos"] = key_features["_key_cos"]
    df["_key_sin"] = key_features["_key_sin"]

    time_sig_features = flag_odd_time_signatures(df)
    df["_time_signature_is_four"] = time_sig_features["_time_signature_is_four"]


    df["_label_size"] = label_to_year_size(df, label_size_buckets, label_size_qsplit)

    if duration_clip_quantile < 1.0:
        upper = df["duration_ms"].quantile(duration_clip_quantile)
        df["duration_ms"] = df["duration_ms"].clip(upper=upper)

    df["_loudness"] = normalize_and_clip_loudness(df)

    df["_artist_genres"] = genres_mask_under_threshold(df["artist_genres"], genre_threshold)
    assert df.isna().sum().sum() == 0
    return (
        df[ENGINEERED_COLUMNS]
        .astype({k: v for k, v in DTYPES.items() if k in ENGINEERED_COLUMNS})
        .reset_index(drop=True)
    )
