from typing import NamedTuple, TypedDict


class TrackEntity(NamedTuple):
    name: str
    key: str
    table: str
    embedding: str
    repr: None
    repr_join: None


class AlbumEntity(NamedTuple):
    name: str
    key: str
    table: str
    embedding: str
    repr: str
    repr_join: str


class ArtistEntity(NamedTuple):
    name: str
    key: str
    table: str
    embedding: str
    repr: str
    repr_join: str


class LabelEntity(NamedTuple):
    name: str
    key: str
    table: str
    embedding: str
    repr: str
    repr_join: str


Entity = TrackEntity | AlbumEntity | ArtistEntity | LabelEntity

TRACK = TrackEntity("track", "track_rowid", "tracks", "track_embedding", None, None)
ALBUM = AlbumEntity("album", "album_rowid", "albums", "album_embedding", "album_repr_tracks", "tracks")
ARTIST = ArtistEntity("artist", "artist_rowid", "artists", "artist_embedding", "artist_repr_albums", "albums")
LABEL = LabelEntity("label", "label_rowid", "labels", "label_embedding", "label_repr_artists", "artists")

ENTITIES = (TRACK, ALBUM, ARTIST, LABEL)
NAME2ENTITY = {e.name: e for e in ENTITIES}


def entity_child(entity: Entity) -> tuple[Entity, ...]:
    if isinstance(entity, TrackEntity):
        return (ALBUM, ARTIST, LABEL)
    if isinstance(entity, AlbumEntity):
        return (ARTIST, LABEL)
    if isinstance(entity, (ArtistEntity, LabelEntity)):
        return tuple()
    raise ValueError(f"Unknown entity: {entity}")


def cols(info_cls: TypedDict) -> list[str]:
    return list(info_cls.__annotations__)
