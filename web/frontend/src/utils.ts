/** Extracts the numeric rowid from any loaded entity. */
export function getRowid(entity: {
  entity_type: 'track' | 'album' | 'artist' | 'label'
  track_rowid?: number
  album_rowid?: number
  artist_rowid?: number
  label_rowid?: number
}): number {
  switch (entity.entity_type) {
    case 'track':
      return entity.track_rowid!
    case 'album':
      return entity.album_rowid!
    case 'artist':
      return entity.artist_rowid!
    case 'label':
      return entity.label_rowid!
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
