import type { EntityType } from "./utils.ts";

export type Entity = {
  entity_type: EntityType;
  rowid: number;
  lon: number;
  lat: number;
  logcount: number;
};

// --- Types mirroring backend TypedDicts ---

export type TrackInfo = Entity & {
  entity_type: "track";
  track_name_norm: string;
  artist_rowid: number;
  artist_name: string;
  artist_logcount: number;
  album_rowid: number;
  album_name: string;
  album_name_norm: string;
  album_logcount: number;
  label_rowid: number;
  label: string;
  label_logcount: number;
  album_lon: number;
  album_lat: number;
  artist_lon: number;
  artist_lat: number;
  label_lon: number;
  label_lat: number;
  release_date: string | null;
};

export type AlbumInfo = Entity & {
  entity_type: "album";
  album_name_norm: string;
  artist_rowid: number;
  artist_name: string;
  artist_logcount: number;
  label_rowid: number;
  label: string;
  label_logcount: number;
  artist_lon: number;
  artist_lat: number;
  label_lon: number;
  label_lat: number;
  nrepr: number;
  total_tracks: number | null;
  release_date: string | null;
  album_type: string | null;
  reprs: TrackRepr[];
};

export type ArtistInfo = Entity & {
  entity_type: "artist";
  artist_name: string;
  ntrack: number;
  nalbum: number;
  nrepr: number;
  artist_genre: string | null;
  reprs: AlbumRepr[];
};

export type LabelInfo = Entity & {
  entity_type: "label";
  label: string;
  ntrack: number;
  nalbum: number;
  nartist: number;
  nrepr: number;
  reprs: ArtistRepr[];
};

export type EntityInfo = TrackInfo | AlbumInfo | ArtistInfo | LabelInfo;

// --- Recommendation types mirroring backend TypedDicts ---

export type TrackRecommend = Entity & {
  entity_type: "track";
  track_name_norm: string;
  artist_name: string;
  simscore: number;
};

export type AlbumRecommend = Entity & {
  entity_type: "album";
  album_name_norm: string;
  artist_name: string;
  simscore: number;
};

export type ArtistRecommend = Entity & {
  entity_type: "artist";
  artist_name: string;
  simscore: number;
  artist_genre: string | null;
};

export type LabelRecommend = Entity & {
  entity_type: "label";
  label: string;
  simscore: number;
};

export type Recommend =
  | TrackRecommend
  | AlbumRecommend
  | ArtistRecommend
  | LabelRecommend;

// --- Repr types mirroring backend TypedDicts ---

export type TrackRepr = Entity & {
  entity_type: "track";
  track_name_norm: string;
  artist_name: string;
};

export type AlbumRepr = Entity & {
  entity_type: "album";
  album_name_norm: string;
  artist_name: string;
};

export type ArtistRepr = Entity & {
  entity_type: "artist";
  artist_name: string;
};

/**
 * Discriminated union representing one entry in the navigation stack.
 * Loading/error keep entity identity so hash sync can stay declarative.
 */
export type Selection =
  | ({ status: "loading" } & Entity)
  | ({ status: "error" } & Entity)
  | ({ status: "loaded"; recs?: Recommend[] } & EntityInfo);

export type LoadedSelection = Extract<Selection, { status: "loaded" }>;

/** Shallow merge into the top of the nav stack when the target entity still matches. */
export type UpdateFn = (
  entityType: EntityType,
  rowid: number,
  patch: Partial<LoadedSelection>,
) => void;
