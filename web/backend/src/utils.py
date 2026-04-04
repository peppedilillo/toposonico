from typing import NamedTuple

Key = str


class Keys(NamedTuple):
    track: Key = "track_rowid"
    album: Key = "album_rowid"
    artist: Key = "artist_rowid"
    label: Key = "label_rowid"

KEYS = Keys()

Table = str

class Tables(NamedTuple):
    track: Table = "tracks"
    album: Table = "albums"
    artist: Table = "artists"
    label: Table = "labels"
    track_embedding: Table = "track_embedding"
    album_embedding: Table = "album_embedding"
    artist_embedding: Table = "artist_embedding"
    label_embedding: Table = "label_embedding"
    album_repr_tracks: Table = "album_repr_tracks"
    artist_repr_albums: Table = "artist_repr_albums"
    label_repr_artists: Table = "label_repr_artists"

TABLES = Tables()


class Entity(NamedTuple):
    key: str
    table: str
    embedding: str
    repr: str | None


TRACK = Entity(
    key=KEYS.track,
    table=TABLES.track,
    embedding=TABLES.track_embedding,
    repr=None,
)
ALBUM = Entity(
    key=KEYS.album,
    table=TABLES.album,
    embedding=TABLES.album_embedding,
    repr=TABLES.album_repr_tracks,
)
ARTIST = Entity(
    key=KEYS.artist,
    table=TABLES.artist,
    embedding=TABLES.artist_embedding,
    repr=TABLES.artist_repr_albums,
)
LABEL = Entity(
    key=KEYS.label,
    table=TABLES.label,
    embedding=TABLES.label_embedding,
    repr=TABLES.label_repr_artists,
)


ENTITIES = (TRACK, ALBUM, ARTIST, LABEL)
ENTITY_TREE = {TRACK: {ALBUM: {ARTIST: {}, LABEL: {}}}}


def entity_child(entity: Entity) -> tuple[Entity, ...]:
    if entity == TRACK:
        return (ALBUM, ARTIST, LABEL)
    if entity == ALBUM:
        return (ARTIST, LABEL)
    if entity in {ARTIST, LABEL}:
        return tuple()
    raise ValueError(f"Unknown entity: {entity}")