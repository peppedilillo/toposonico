import os
import sqlite3

import meilisearch


def get_config_str(var: str) -> str:
    val = os.environ.get(var)
    if val is None:
        raise ValueError("")
    return val


MEILI_URL = get_config_str("MEILI_URL")
MEILI_UID = get_config_str("MEILI_UID")
MEILI_KEY = get_config_str("MEILI_KEY")
SICK_DB = get_config_str("SICK_DB")
meili_client = meilisearch.Client(MEILI_URL, MEILI_KEY)
meili_index = meili_client.index(MEILI_UID)
sick_db = sqlite3.connect(f"file:{SICK_DB}?mode=ro", uri=True, check_same_thread=False)
