/** Maps entity type to its theme color CSS variable value. */
const ENTITY_COLORS: Record<string, string> = {
  track: 'var(--color-track)',
  album: 'var(--color-album)',
  artist: 'var(--color-artist)',
  label: 'var(--color-label)',
}

type BadgeProps = { entityType: 'track' | 'album' | 'artist' | 'label' }

/** Colored pill displaying the entity type. */
export default function Badge({entityType}: BadgeProps) {
  return (
    <span
      className="uppercase tracking-wider text-[10px] px-1.5 py-0.5 rounded font-semibold"
      style={{color: ENTITY_COLORS[entityType], background: 'rgba(255,255,255,0.07)'}}
    >
      {entityType}
    </span>
  )
}
