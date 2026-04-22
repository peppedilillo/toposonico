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

/** Standard format for blank track names. **/
const NO_TRACK_NAME = "[no track name]";

export function displayTrackName(trackNameNorm: string) {
  return trackNameNorm || NO_TRACK_NAME;
}
