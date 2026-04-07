const palette = {
  black: '#000000',
  nearBlack: '#191919',
  darkGray: '#303030',
  gray: '#7e7d7d',
  white: '#ffffff',
  track: '#30ba1f',
  artist: '#30ba1f',
  album: '#30ba1f',
  label: '#8f45ae',
}

const colors = {
  background: palette.black,
  surface: palette.darkGray,
  border: palette.darkGray,
  foreground: palette.white,
  muted: palette.gray,
  overlay: 'rgba(25, 25, 25, 0.8)',
  track: palette.track,
  artist: palette.artist,
  album: palette.album,
  label: palette.label,
}

export default colors

export function rgba(hex: string, alpha: number = 255): [number, number, number, number] {
  const n = parseInt(hex.slice(1), 16)
  return [(n >> 16) & 255, (n >> 8) & 255, n & 255, alpha]
}
