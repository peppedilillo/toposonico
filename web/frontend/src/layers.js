import colors from './theme.js'

const LAYERS = [
  {
    id: 'tracks',
    sourceLayer: 'tracks',
    rowidProp: 'track_rowid',
    radius: 1.0,
    color: colors.track,
    opacity: 0.7,
  },
  {
    id: 'albums',
    sourceLayer: 'albums',
    rowidProp: 'album_rowid',
    radius: 1.0,
    color: colors.album,
    opacity: 0.7,
  },
  {
    id: 'artists',
    sourceLayer: 'artists',
    rowidProp: 'artist_rowid',
    radius: 1.0,
    color: colors.artist,
    opacity: 0.7,
  },
  {
    id: 'labels',
    sourceLayer: 'labels',
    rowidProp: 'label_rowid',
    radius: 1.0,
    color: colors.label,
    opacity: 0.7,
  },
]

export {LAYERS}
