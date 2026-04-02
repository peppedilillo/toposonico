import colors from './theme.js'

const LAYERS = [
  {
    id: 'tracks',
    sourceLayer: 'tracks',
    rowidProp: 'track_rowid',
    radius: 1.0,
    color: colors.track,
    opacity: 1.0,
  },
  {
    id: 'albums',
    sourceLayer: 'albums',
    rowidProp: 'album_rowid',
    radius: 1.0,
    color: colors.album,
    opacity: 1.0,
  },
  {
    id: 'artists',
    sourceLayer: 'artists',
    rowidProp: 'artist_rowid',
    radius: 1.0,
    color: colors.artist,
    opacity: 1.0,
  },
  {
    id: 'labels',
    sourceLayer: 'labels',
    rowidProp: 'label_rowid',
    radius: 1.0,
    color: colors.label,
    opacity: 1.0,
  },
]

export {LAYERS}
