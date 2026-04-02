import json
import sqlite3

from scripts.build_db import DDL
from scripts.build_geojson import build_entity


def test_build_entity_writes_ndjson_features(tmp_path):
    conn = sqlite3.connect(":memory:")
    conn.executescript(DDL)
    conn.execute("""
        INSERT INTO tracks (
            track_rowid, track_canonical_rowid, track_name, track_popularity,
            logcount, release_date, id_isrc, searchable, recable, lon, lat,
            artist_rowid, artist_name, artist_lon, artist_lat,
            album_rowid, album_name, album_lon, album_lat,
            label_rowid, label, label_lon, label_lat
        ) VALUES (
            1001, 1001, 'Track', 50,
            3.1, '2024-01-01', 'ISRC001', 1, 1, 0.1, 1.1,
            101, 'Artist', 5.0, 50.0,
            201, 'Album', 7.0, 70.0,
            1, 'Label', 1.0, 10.0
        )
        """)
    conn.commit()

    track_out = tmp_path / "track.ndjson"
    build_entity(conn, "tracks", "track_rowid", track_out)
    conn.close()

    lines = track_out.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1

    feature = json.loads(lines[0])
    assert feature["type"] == "Feature"
    assert feature["geometry"] == {"type": "Point", "coordinates": [0.1, 1.1]}
    assert feature["properties"] == {"track_rowid": 1001, "logcount": 3.1}
