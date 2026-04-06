from __future__ import annotations

from typing import NamedTuple, TypedDict


class TrackEntity(NamedTuple):
    name: str
    key: str
    table: str
    embedding: str


class AlbumEntity(NamedTuple):
    name: str
    key: str
    table: str
    embedding: str
    repr: str
    repr_entity: Entity


class ArtistEntity(NamedTuple):
    name: str
    key: str
    table: str
    embedding: str
    repr: str
    repr_entity: Entity


class LabelEntity(NamedTuple):
    name: str
    key: str
    table: str
    embedding: str
    repr: str
    repr_entity: Entity


Entity = TrackEntity | AlbumEntity | ArtistEntity | LabelEntity

TRACK = TrackEntity("track", "track_rowid", "tracks", "track_embedding",)
ALBUM = AlbumEntity(
    "album",
    "album_rowid",
    "albums",
    "album_embedding",
    "album_repr_tracks",
    TRACK,
)
ARTIST = ArtistEntity(
    "artist",
    "artist_rowid",
    "artists",
    "artist_embedding",
    "artist_repr_albums",
    ALBUM,
)
LABEL = LabelEntity(
    "label",
    "label_rowid",
    "labels",
    "label_embedding",
    "label_repr_artists",
    ARTIST,
)

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
