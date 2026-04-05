import os
import sqlite3
import tempfile

import faiss

# Create a valid (empty) SQLite file so the module-level connect in src.main succeeds.
_tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_tmp.close()
sqlite3.connect(_tmp.name).close()

os.environ.setdefault("MEILI_URL", "http://localhost:7700")
os.environ.setdefault("MEILI_UID", "sick")
os.environ.setdefault("MEILI_KEY", "test")
os.environ.setdefault("SICK_DB", _tmp.name)

for _var in ("SICK_FAISS_TRACK", "SICK_FAISS_ALBUM", "SICK_FAISS_ARTIST", "SICK_FAISS_LABEL"):
    _idx_tmp = tempfile.NamedTemporaryFile(suffix=".index", delete=False)
    _idx_tmp.close()
    _idx = faiss.IndexFlatIP(1)
    faiss.write_index(_idx, _idx_tmp.name)
    os.environ.setdefault(_var, _idx_tmp.name)
