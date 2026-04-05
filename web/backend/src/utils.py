from typing import NamedTuple


class TrackEntity(NamedTuple):
    name: str
    key: str
    table: str
    embedding: str
    repr: None


class AlbumEntity(NamedTuple):
    name: str
    key: str
    table: str
    embedding: str
    repr: str


class ArtistEntity(NamedTuple):
    name: str
    key: str
    table: str
    embedding: str
    repr: str


class LabelEntity(NamedTuple):
    name: str
    key: str
    table: str
    embedding: str
    repr: str


Entity = TrackEntity | AlbumEntity | ArtistEntity | LabelEntity

TRACK = TrackEntity("track", "track_rowid", "tracks", "track_embedding", None)
ALBUM = AlbumEntity("album", "album_rowid", "albums", "album_embedding", "album_repr_tracks")
ARTIST = ArtistEntity("artist", "artist_rowid", "artists", "artist_embedding", "artist_repr_albums")
LABEL = LabelEntity("label", "label_rowid", "labels", "label_embedding", "label_repr_artists")

ENTITIES = (TRACK, ALBUM, ARTIST, LABEL)
KEY2ENTITY = {e.key: e for e in ENTITIES}


def entity_child(entity: Entity) -> tuple[Entity, ...]:
    if isinstance(entity, TrackEntity):
        return (ALBUM, ARTIST, LABEL)
    if isinstance(entity, AlbumEntity):
        return (ARTIST, LABEL)
    if isinstance(entity, (ArtistEntity, LabelEntity)):
        return tuple()
    raise ValueError(f"Unknown entity: {entity}")
