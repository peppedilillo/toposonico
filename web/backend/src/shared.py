import os
import sqlite3

import meilisearch
import faiss


def get_config_str(var: str) -> str:
    val = os.environ.get(var)
    if val is None:
        raise ValueError("")
    return val


MEILI_URL = get_config_str("MEILI_URL")
MEILI_UID = get_config_str("MEILI_UID")
MEILI_KEY = get_config_str("MEILI_KEY")
FAISS_TRACK = get_config_str("SICK_FAISS_TRACK")
FAISS_ALBUM = get_config_str("SICK_FAISS_ALBUM")
FAISS_ARTIST = get_config_str("SICK_FAISS_ARTIST")
FAISS_LABEL = get_config_str("SICK_FAISS_LABEL")
DB = get_config_str("SICK_DB")

meili_client = meilisearch.Client(MEILI_URL, MEILI_KEY)
meili_index = meili_client.index(MEILI_UID)
faiss_track_index = faiss.read_index(FAISS_TRACK)
faiss_album_index = faiss.read_index(FAISS_ALBUM)
faiss_artist_index = faiss.read_index(FAISS_ARTIST)
faiss_label_index = faiss.read_index(FAISS_LABEL)
sick_db = sqlite3.connect(f"file:{DB}?mode=ro", uri=True, check_same_thread=False)
