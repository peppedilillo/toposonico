/** Extracts the numeric rowid from any loaded entity. */
type RowidCarrier =
  | { entity_type: 'track';  track_rowid: number }
  | { entity_type: 'album';  album_rowid: number }
  | { entity_type: 'artist'; artist_rowid: number }
  | { entity_type: 'label';  label_rowid: number }

export function getRowid(e: RowidCarrier): number {
  switch (e.entity_type) {
    case 'track':  return e.track_rowid
    case 'album':  return e.album_rowid
    case 'artist': return e.artist_rowid
    case 'label':  return e.label_rowid
  }
}

/** Formats a number with K/M/B suffix (e.g. 7436313 -> "7.4M"). */
export function humanCount(n: number): string {
  if (n >= 1e9) return (n / 1e9).toFixed(1).replace(/\.0$/, '') + 'B'
  if (n >= 1e6) return (n / 1e6).toFixed(1).replace(/\.0$/, '') + 'M'
  if (n >= 1e3) return (n / 1e3).toFixed(1).replace(/\.0$/, '') + 'K'
  return String(n)
}

/** Converts the stored log-count back to an approximate playlist count. */
export function formatPlaylistCount(logcount: number): string {
  return `${humanCount(Math.round(10 ** logcount))} playlists`
}
