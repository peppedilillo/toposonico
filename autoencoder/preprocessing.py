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

    df = df.astype({k: v for k, v in DTYPES.items() if k in df.columns})
    return df
