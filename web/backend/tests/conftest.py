import os
import sqlite3
import tempfile

# Create a valid (empty) SQLite file so the module-level connect in src.main succeeds.
_tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_tmp.close()
sqlite3.connect(_tmp.name).close()

os.environ.setdefault("MEILI_URL", "http://localhost:7700")
os.environ.setdefault("MEILI_UID", "sick")
os.environ.setdefault("MEILI_KEY", "test")
os.environ.setdefault("SICK_DB", _tmp.name)
