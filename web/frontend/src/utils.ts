/** Formats a number with K/M/B suffix (e.g. 7436313 -> "7.4M"). */
export function humanCount(n: number): string {
  if (n >= 1e9) return (n / 1e9).toFixed(1).replace(/\.0$/, "") + "B";
  if (n >= 1e6) return (n / 1e6).toFixed(1).replace(/\.0$/, "") + "M";
  if (n >= 1e3) return (n / 1e3).toFixed(1).replace(/\.0$/, "") + "K";
  return String(n);
}

/** Converts the stored log-count back to an approximate playlist count. */
export function formatPlaylistCount(logcount: number): string {
  return `${humanCount(Math.round(10 ** logcount))} playlists`;
}

export type EntityType = "track" | "album" | "artist" | "label";

function parseEnvZoom(name: string, value: string | undefined): number {
  if (value == null || value.trim() === "") {
    throw new Error(`Missing required env var ${name}`);
  }

  const zoom = Number(value);
  if (!Number.isFinite(zoom)) {
    throw new Error(
      `Invalid ${name}: expected a finite number, got ${JSON.stringify(value)}`,
    );
  }
  return zoom;
}

export const ENTITY_BASE_ZOOMS: Record<EntityType, number> = {
  track: parseEnvZoom(
    "VITE_BASE_ZOOM_TRACK",
    import.meta.env.VITE_BASE_ZOOM_TRACK,
  ),
  album: parseEnvZoom(
    "VITE_BASE_ZOOM_ALBUM",
    import.meta.env.VITE_BASE_ZOOM_ALBUM,
  ),
  artist: parseEnvZoom(
    "VITE_BASE_ZOOM_ARTIST",
    import.meta.env.VITE_BASE_ZOOM_ARTIST,
  ),
  label: parseEnvZoom(
    "VITE_BASE_ZOOM_LABEL",
    import.meta.env.VITE_BASE_ZOOM_LABEL,
  ),
};

export const SOURCE_MAX_ZOOM = parseEnvZoom(
  "VITE_SOURCE_MAX_ZOOM",
  import.meta.env.VITE_SOURCE_MAX_ZOOM,
);

/** Standard format for blank track names. **/
const NO_TRACK_NAME = "[no track name]";

export function displayTrackName(trackNameNorm: string) {
  return trackNameNorm || NO_TRACK_NAME;
}
