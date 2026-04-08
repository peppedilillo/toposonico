import {useRef, useState} from 'react'
import Badge from './Badge'
import {makeAbortable} from './requests'

// --- Types mirroring backend TypedDicts ---

type TrackInfo = {
  entity_type: 'track'
  track_rowid: number
  track_name: string
  artist_rowid: number
  artist_name: string
  album_rowid: number
  album_name: string
  label_rowid: number
  label: string
  lon: number
  lat: number
  album_lon: number
  album_lat: number
  artist_lon: number
  artist_lat: number
  label_lon: number
  label_lat: number
  logcount: number
  release_date: string | null
}

type AlbumInfo = {
  entity_type: 'album'
  album_rowid: number
  album_name_norm: string
  artist_rowid: number
  artist_name: string
  label_rowid: number
  label: string
  lon: number
  lat: number
  artist_lon: number
  artist_lat: number
  label_lon: number
  label_lat: number
  logcount: number
  nrepr: number
  total_tracks: number | null
  release_date: string | null
  album_type: string | null
}

type ArtistInfo = {
  entity_type: 'artist'
  artist_rowid: number
  artist_name: string
  lon: number
  lat: number
  logcount: number
  ntrack: number
  nalbum: number
  nrepr: number
  artist_genre: string | null
}

type LabelInfo = {
  entity_type: 'label'
  label_rowid: number
  label: string
  lon: number
  lat: number
  logcount: number
  ntrack: number
  nalbum: number
  nartist: number
  nrepr: number
}

type EntityInfo = TrackInfo | AlbumInfo | ArtistInfo | LabelInfo

// --- Recommendation types mirroring backend TypedDicts ---

type TrackRecommend = { track_rowid: number; track_name: string; artist_name: string; lon: number; lat: number }
type AlbumRecommend = { album_rowid: number; album_name_norm: string; artist_name: string; lon: number; lat: number }
type ArtistRecommend = { artist_rowid: number; artist_name: string; lon: number; lat: number }
type LabelRecommend = { label_rowid: number; label: string; lon: number; lat: number }
type Recommend = TrackRecommend | AlbumRecommend | ArtistRecommend | LabelRecommend

type RecsState =
  | {status: 'loading'}
  | {status: 'error'}
  | {status: 'loaded'; items: Recommend[]}

/** Discriminated union representing the panel's async state. */
export type Selection =
  | {status: 'loading'}
  | {status: 'error'}
  | ({status: 'loaded'} & EntityInfo)

type NavigateFn = (entityType: string, rowid: number, lon: number, lat: number) => void

type PanelProps = {
  selection: Selection | null
  navigate: NavigateFn
  onClose: () => void
}

// --- Internal sub-components ---

/** Navigable inline link styled with a per-entity color on hover. */
function Link({onClick, color, children}: {onClick: () => void; color: string; children: React.ReactNode}) {
  const [hovered, setHovered] = useState(false)
  return (
    <button
      onClick={(e) => {e.stopPropagation(); onClick()}}
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
      style={hovered ? {color} : undefined}
      className="cursor-pointer text-left max-w-full truncate transition-colors"
    >
      {children}
    </button>
  )
}

/** Animated placeholder shown while entity info is loading. */
function LoadingBody() {
  return (
    <div className="space-y-2 animate-pulse mt-1">
      <div className="h-5 bg-muted/20 rounded w-3/4"/>
      <div className="h-4 bg-muted/20 rounded w-1/2"/>
      <div className="h-3 bg-muted/20 rounded w-2/3"/>
      <div className="h-3 bg-muted/20 rounded w-1/3"/>
    </div>
  )
}

/** Formats a logcount value as an approximate playlist count. */
function PlaylistCount({logcount}: {logcount: number}) {
  return (
    <span className="bg-muted/10 px-1.5 py-0.5 rounded">
      {Math.round(10 ** logcount).toLocaleString()} playlists
    </span>
  )
}

function TrackPanel({s, navigate}: {s: TrackInfo; navigate: NavigateFn}) {
  return (
    <>
      <div className="text-lg font-semibold leading-snug truncate">{s.track_name}</div>
      <div className="italic font-medium leading-tight text-sm mt-0.5 truncate">
        <Link onClick={() => navigate('artist', s.artist_rowid, s.artist_lon, s.artist_lat)} color="var(--color-artist)">{s.artist_name}</Link>
      </div>
      {s.album_name && (
        <div className="text-sm mt-1 truncate">
          <Link onClick={() => navigate('album', s.album_rowid, s.album_lon, s.album_lat)} color="var(--color-album)">{s.album_name}</Link>
        </div>
      )}
      {s.label && (
        <div className="text-xs text-muted mt-0.5 truncate">
          <Link onClick={() => navigate('label', s.label_rowid, s.label_lon, s.label_lat)} color="var(--color-label)">{s.label}</Link>
        </div>
      )}
      <div className="flex items-center gap-2 mt-2 text-xs text-muted">
        {s.release_date && <span>{s.release_date.slice(0, 4)}</span>}
        <PlaylistCount logcount={s.logcount}/>
        <span className="absolute right-4 bottom-auto">id:{s.track_rowid}</span>
      </div>
    </>
  )
}

function AlbumPanel({s, navigate}: {s: AlbumInfo; navigate: NavigateFn}) {
  return (
    <>
      <div className="text-lg font-semibold leading-snug truncate">{s.album_name_norm}</div>
      {s.artist_name && (
        <div className="italic font-medium leading-tight text-sm mt-0.5 truncate">
          <Link onClick={() => navigate('artist', s.artist_rowid, s.artist_lon, s.artist_lat)} color="var(--color-artist)">{s.artist_name}</Link>
        </div>
      )}
      {s.label && (
        <div className="text-xs text-muted mt-0.5 truncate">
          <Link onClick={() => navigate('label', s.label_rowid, s.label_lon, s.label_lat)} color="var(--color-label)">{s.label}</Link>
        </div>
      )}
      <div className="flex items-center gap-2 mt-2 text-xs text-muted">
        {s.release_date && <span>{s.release_date.slice(0, 4)}</span>}
        <PlaylistCount logcount={s.logcount}/>
        <span className="absolute right-4 bottom-auto">id:{s.album_rowid}</span>
      </div>
    </>
  )
}

function ArtistPanel({s}: {s: ArtistInfo}) {
  return (
    <>
      <div className="text-lg font-semibold leading-snug truncate">{s.artist_name}</div>
      {s.artist_genre && <div className="text-xs text-muted truncate">{s.artist_genre}</div>}
      <div className="flex items-center gap-2 mt-2 text-xs text-muted">
        <PlaylistCount logcount={s.logcount}/>
        <span className="absolute right-4 bottom-auto">id:{s.artist_rowid}</span>
      </div>
    </>
  )
}

function LabelPanel({s}: {s: LabelInfo}) {
  return (
    <>
      <div className="text-lg font-semibold leading-snug truncate">{s.label}</div>
      <div className="flex items-center gap-2 mt-2 text-xs text-muted">
        <PlaylistCount logcount={s.logcount}/>
        <span className="absolute right-4 bottom-auto">id:{s.label_rowid}</span>
      </div>
    </>
  )
}

// --- Recommendation helpers and components ---

/** Extracts the rowid from any loaded entity info. */
function getRowid(s: EntityInfo): number {
  switch (s.entity_type) {
    case 'track':  return s.track_rowid
    case 'album':  return s.album_rowid
    case 'artist': return s.artist_rowid
    case 'label':  return s.label_rowid
  }
}

/** Extracts navigation args from a recommendation. */
function getRecNav(rec: Recommend, entityType: string): [string, number, number, number] {
  switch (entityType) {
    case 'track':  { const r = rec as TrackRecommend;  return ['track',  r.track_rowid,  r.lon, r.lat] }
    case 'album':  { const r = rec as AlbumRecommend;  return ['album',  r.album_rowid,  r.lon, r.lat] }
    case 'artist': { const r = rec as ArtistRecommend; return ['artist', r.artist_rowid, r.lon, r.lat] }
    default:       { const r = rec as LabelRecommend;  return ['label',  r.label_rowid,  r.lon, r.lat] }
  }
}

/** Returns display name and optional subtitle for a recommendation. */
function getRecDisplay(rec: Recommend, entityType: string): {name: string; sub?: string} {
  switch (entityType) {
    case 'track':  return {name: (rec as TrackRecommend).track_name,      sub: (rec as TrackRecommend).artist_name}
    case 'album':  return {name: (rec as AlbumRecommend).album_name_norm, sub: (rec as AlbumRecommend).artist_name}
    case 'artist': return {name: (rec as ArtistRecommend).artist_name}
    default:       return {name: (rec as LabelRecommend).label}
  }
}

/** Single recommendation row — full-width clickable button. */
function RecItem({rec, entityType, navigate}: {rec: Recommend; entityType: string; navigate: NavigateFn}) {
  const {name, sub} = getRecDisplay(rec, entityType)
  const [et, rowid, lon, lat] = getRecNav(rec, entityType)
  return (
    <li>
      <button
        onClick={(e) => { e.stopPropagation(); navigate(et, rowid, lon, lat) }}
        className="text-left w-full cursor-pointer hover:bg-overlay py-1.5 px-4"
      >
        <div className="text-sm font-medium truncate">{name}</div>
        {sub && <div className="text-xs text-muted truncate">{sub}</div>}
      </button>
    </li>
  )
}

/** Renders loading/error/empty/list states for recommendations. */
function RecBody({recs, entityType, navigate}: {recs: RecsState | null; entityType: string; navigate: NavigateFn}) {
  if (!recs || recs.status === 'loading')
    return <div className="text-muted text-xs py-2 animate-pulse">Loading...</div>
  if (recs.status === 'error')
    return <div className="text-muted text-xs py-2">Failed to load.</div>
  if (recs.items.length === 0)
    return <div className="text-muted text-xs py-2">No recommendations.</div>
  return (
    <ol className="mt-1">
      {recs.items.map((rec, i) => (
        <RecItem key={i} rec={rec} entityType={entityType} navigate={navigate}/>
      ))}
    </ol>
  )
}

/**
 * Collapsible recommendations section with fetch-on-open and caching.
 * Keyed by entity identity in the parent — remounts on entity change.
 */
function RecsSection({entity, navigate}: {entity: EntityInfo; navigate: NavigateFn}) {
  const [open, setOpen] = useState(false)
  const [recs, setRecs] = useState<RecsState | null>(null)
  const aborter = useRef(makeAbortable())
  const rowid = getRowid(entity)
  const RECSNUMBER = 3

  const handleToggle = () => {
    if (open) { setOpen(false); return }
    setOpen(true)
    if (recs !== null) return
    setRecs({status: 'loading'})
    const signal = aborter.current.nextSignal()
    fetch(`/api/recommend?rowid=${rowid}&entity_name=${entity.entity_type}&limit=${RECSNUMBER}&diverse=true`, {signal})
      .then(r => { if (!r.ok) throw new Error(r.statusText); return r.json() })
      .then((data: Recommend[]) => setRecs({status: 'loaded', items: data}))
      .catch(err => { if (err.name !== 'AbortError') setRecs({status: 'error'}) })
  }

  return (
    <div className="mt-3 border-t border-muted/20 pt-2">
      <div
        onClick={handleToggle}
        className={`text-xs flex items-center gap-1 cursor-pointer w-full px-4 py-1 -my-1 select-none
        bg-linear-to-r from-gray-500 via-gray-50 to-gray-500 bg-size-[200%_auto] bg-clip-text text-transparent
        ${!open ? 'animate-sweep' : ''}`}
      >
        More like this..
      </div>
      {open && <RecBody recs={recs} entityType={entity.entity_type} navigate={navigate}/>}
    </div>
  )
}

// --- Main Panel ---

/** Detail panel for a selected entity — bottom sheet on mobile, sidebar on desktop. */
export default function Panel({selection, navigate, onClose}: PanelProps) {
  if (!selection) return null

  let body: React.ReactNode
  if (selection.status === 'loading') {
    body = <LoadingBody/>
  } else if (selection.status === 'error') {
    body = <div className="text-muted text-sm mt-1">Failed to load.</div>
  } else if (selection.entity_type === 'track') {
    body = <><div className="mb-2"><Badge entityType="track"/></div><TrackPanel s={selection} navigate={navigate}/></>
  } else if (selection.entity_type === 'album') {
    body = <><div className="mb-2"><Badge entityType="album"/></div><AlbumPanel s={selection} navigate={navigate}/></>
  } else if (selection.entity_type === 'artist') {
    body = <><div className="mb-2"><Badge entityType="artist"/></div><ArtistPanel s={selection}/></>
  } else {
    body = <><div className="mb-2"><Badge entityType="label"/></div><LabelPanel s={selection}/></>
  }

  return (
    <div
      className="fixed z-10 bottom-0 left-0 right-0
                 sm:bottom-4 sm:left-3 sm:top-auto sm:right-auto sm:w-md
                 max-h-[60dvh] sm:max-h-[calc(100vh-6rem)]
                 overflow-y-auto overscroll-contain
                 bg-surface font-sans text-base text-white
                 rounded-t-2xl sm:rounded-xl shadow-xl touch-auto"
      style={{paddingBottom: 'calc(1.25rem + env(safe-area-inset-bottom))'}}
      onPointerDown={(e) => e.stopPropagation()}
      onPointerMove={(e) => e.stopPropagation()}
    >
      <div className="flex items-start gap-3">
        <div className="flex-1 min-w-0 px-4 pt-4">
          {body}
        </div>
        <button
          onClick={onClose}
          className="text-muted hover:text-white transition-colors text-lg leading-none flex-shrink-0 mt-0.5 p-4"
          aria-label="Close"
        >×</button>
      </div>
      {selection.status === 'loaded' && (
        <RecsSection key={`${selection.entity_type}:${getRowid(selection)}`} entity={selection} navigate={navigate}/>
      )}
    </div>
  )
}
