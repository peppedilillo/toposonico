import type { EntityType } from "./utils.ts";

// --- Types mirroring backend TypedDicts ---

export type TrackInfo = {
  entity_type: "track";
  track_rowid: number;
  track_name_norm: string;
  artist_rowid: number;
  artist_name: string;
  album_rowid: number;
  album_name: string;
  album_name_norm: string;
  label_rowid: number;
  label: string;
  lon: number;
  lat: number;
  album_lon: number;
  album_lat: number;
  artist_lon: number;
  artist_lat: number;
  label_lon: number;
  label_lat: number;
  logcount: number;
  release_date: string | null;
};

export type AlbumInfo = {
  entity_type: "album";
  album_rowid: number;
  album_name_norm: string;
  artist_rowid: number;
  artist_name: string;
  label_rowid: number;
  label: string;
  lon: number;
  lat: number;
  artist_lon: number;
  artist_lat: number;
  label_lon: number;
  label_lat: number;
  logcount: number;
  nrepr: number;
  total_tracks: number | null;
  release_date: string | null;
  album_type: string | null;
  reprs: TrackRepr[];
};

export type ArtistInfo = {
  entity_type: "artist";
  artist_rowid: number;
  artist_name: string;
  lon: number;
  lat: number;
  logcount: number;
  ntrack: number;
  nalbum: number;
  nrepr: number;
  artist_genre: string | null;
  reprs: AlbumRepr[];
};

export type LabelInfo = {
  entity_type: "label";
  label_rowid: number;
  label: string;
  lon: number;
  lat: number;
  logcount: number;
  ntrack: number;
  nalbum: number;
  nartist: number;
  nrepr: number;
  reprs: ArtistRepr[];
};

export type EntityInfo = TrackInfo | AlbumInfo | ArtistInfo | LabelInfo;

// --- Recommendation types mirroring backend TypedDicts ---

export type TrackRecommend = {
  track_rowid: number;
  track_name_norm: string;
  artist_name: string;
  lon: number;
  lat: number;
  logcount: number;
  simscore: number;
};

export type AlbumRecommend = {
  album_rowid: number;
  album_name_norm: string;
  artist_name: string;
  lon: number;
  lat: number;
  logcount: number;
  simscore: number;
};

export type ArtistRecommend = {
  artist_rowid: number;
  artist_name: string;
  lon: number;
  lat: number;
  logcount: number;
  simscore: number;
  artist_genre: string | null;
};

export type LabelRecommend = {
  label_rowid: number;
  label: string;
  lon: number;
  lat: number;
  logcount: number;
  simscore: number;
};

export type Recommend =
  | TrackRecommend
  | AlbumRecommend
  | ArtistRecommend
  | LabelRecommend;

// --- Repr types mirroring backend TypedDicts ---

export type TrackRepr = {
  track_rowid: number;
  track_name_norm: string;
  artist_name: string;
  lon: number;
  lat: number;
};

export type AlbumRepr = {
  album_rowid: number;
  album_name_norm: string;
  artist_name: string;
  lon: number;
  lat: number;
};

export type ArtistRepr = {
  artist_rowid: number;
  artist_name: string;
  lon: number;
  lat: number;
};

/**
 * Discriminated union representing one entry in the navigation stack.
 * Loading/error keep entity identity so hash sync can stay declarative.
 */
export type Selection =
  | { status: "loading"; entity_type: EntityType; rowid: number }
  | { status: "error"; entity_type: EntityType; rowid: number }
  | ({ status: "loaded"; recs?: Recommend[] } & EntityInfo);

export type LoadedSelection = Extract<Selection, { status: "loaded" }>;

/** Shallow merge into the top of the nav stack when the target entity still matches. */
export type UpdateFn = (
  entityType: EntityType,
  rowid: number,
  patch: Partial<LoadedSelection>,
) => void;
