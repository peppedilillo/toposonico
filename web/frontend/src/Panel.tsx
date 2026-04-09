import {useCallback, useEffect, useRef, useState} from 'react'
import Badge from './Badge'
import {makeAbortable} from './requests'

/** Formats a number with K/M/B suffix (e.g. 7436313 → "7.4M"). */
function humanCount(n: number): string {
  if (n >= 1e9) return (n / 1e9).toFixed(1).replace(/\.0$/, '') + 'B'
  if (n >= 1e6) return (n / 1e6).toFixed(1).replace(/\.0$/, '') + 'M'
  if (n >= 1e3) return (n / 1e3).toFixed(1).replace(/\.0$/, '') + 'K'
  return String(n)
}

// --- Types mirroring backend TypedDicts ---

type TrackInfo = {
  entity_type: 'track'
  track_rowid: number
  track_name_norm: string
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
  reprs: TrackRepr[]
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
  reprs: AlbumRepr[]
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
  reprs: ArtistRepr[]
}

type EntityInfo = TrackInfo | AlbumInfo | ArtistInfo | LabelInfo

// --- Recommendation types mirroring backend TypedDicts ---

type TrackRecommend = {
  track_rowid: number
  track_name_norm: string
  artist_name: string
  lon: number
  lat: number
  logcount: number
  simscore: number
}

type AlbumRecommend = {
  album_rowid: number
  album_name_norm: string
  artist_name: string
  lon: number
  lat: number
  logcount: number
  simscore: number
}

type ArtistRecommend = {
  artist_rowid: number
  artist_name: string
  lon: number
  lat: number
  logcount: number
  simscore: number
  artist_genre: string | null
}

type LabelRecommend = {
  label_rowid: number
  label: string
  lon: number
  lat: number
  logcount: number
  simscore: number
}

type Recommend = TrackRecommend | AlbumRecommend | ArtistRecommend | LabelRecommend

// --- Repr types mirroring backend TypedDicts ---

type TrackRepr = {
  track_rowid: number
  track_name_norm: string
  artist_name: string
  lon: number
  lat: number
}

type AlbumRepr = {
  album_rowid: number
  album_name_norm: string
  artist_name: string
  lon: number
  lat: number
}

type ArtistRepr = {
  artist_rowid: number
  artist_name: string
  lon: number
  lat: number
}

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

/** Horizontal scrollable row with wheel-to-scroll and gradient overflow fades. */
function ReprRow({children}: {children: React.ReactNode}) {
  const ref = useRef<HTMLDivElement>(null)
  const [canScrollLeft, setCanScrollLeft] = useState(false)
  const [canScrollRight, setCanScrollRight] = useState(false)

  const updateFades = useCallback(() => {
    const el = ref.current
    if (!el) return
    setCanScrollLeft(el.scrollLeft > 0)
    setCanScrollRight(el.scrollLeft < el.scrollWidth - el.clientWidth - 1)
  }, [])

  useEffect(() => {
    const el = ref.current
    if (!el) return
    updateFades()
    const observer = new ResizeObserver(updateFades)
    observer.observe(el)
    return () => observer.disconnect()
  }, [updateFades])

  const maskImage =
    canScrollLeft && canScrollRight
      ? 'linear-gradient(to right, transparent, black 24px, black calc(100% - 24px), transparent)'
      : canScrollRight
        ? 'linear-gradient(to right, black calc(100% - 24px), transparent)'
        : canScrollLeft
          ? 'linear-gradient(to right, transparent, black 24px)'
          : undefined

  return (
    <div
      ref={ref}
      onScroll={updateFades}
      onWheel={(e) => {
        const el = ref.current
        if (!el || e.deltaY === 0) return
        el.scrollLeft += e.deltaY
        e.preventDefault()
      }}
      className="overflow-x-auto whitespace-nowrap no-scrollbar text-sm mt-1"
      style={{maskImage, WebkitMaskImage: maskImage}}
    >
      {children}
    </div>
  )
}

/** Debug-only rowid display, right-aligned on the playlist count line. Remove for production. */
function DebugId({id}: {id: number}) {
  return <span className="text-[10px] text-muted/50 float-right">id:{id}</span>
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

function TrackPanel({s, navigate}: {s: TrackInfo; navigate: NavigateFn}) {
  return (
    <>
      <div className="mb-2"><Badge entityType="track"/></div>
      <div className="text-lg font-semibold leading-snug truncate">{s.track_name_norm}</div>
      <div className="italic font-medium leading-tight text-sm mt-0.5 truncate">
        <Link onClick={() => navigate('artist', s.artist_rowid, s.artist_lon, s.artist_lat)} color="var(--color-artist)">{s.artist_name}</Link>
      </div>
      {s.album_name && (
        <div className="text-sm mt-1 truncate">
          <Link onClick={() => navigate('album', s.album_rowid, s.album_lon, s.album_lat)} color="var(--color-album)">{s.album_name}</Link>
        </div>
      )}
      {(s.label || s.release_date) && (
        <div className="text-sm text-muted mt-0.5 truncate">
          {s.label && <Link onClick={() => navigate('label', s.label_rowid, s.label_lon, s.label_lat)} color="var(--color-label)">{s.label}</Link>}
          {s.label && s.release_date && ' · '}{s.release_date && <span>{s.release_date.slice(0, 4)}</span>}
        </div>
      )}
      <div className="text-sm text-muted mt-0.5 truncate">
        {humanCount(Math.round(10 ** s.logcount))} playlists
        <DebugId id={s.track_rowid}/>
      </div>
    </>
  )
}

function AlbumPanel({s, navigate}: {s: AlbumInfo; navigate: NavigateFn}) {
  return (
    <>
      <div className="mb-2"><Badge entityType="album"/></div>
      <div className="text-lg font-semibold leading-snug truncate">{s.album_name_norm}</div>
      {s.artist_name && (
        <div className="italic font-medium leading-tight text-sm mt-0.5 truncate">
          <Link onClick={() => navigate('artist', s.artist_rowid, s.artist_lon, s.artist_lat)} color="var(--color-artist)">{s.artist_name}</Link>
        </div>
      )}
      {(s.label || s.release_date) && (
        <div className="text-sm text-muted mt-0.5 truncate">
          {s.label && <Link onClick={() => navigate('label', s.label_rowid, s.label_lon, s.label_lat)} color="var(--color-label)">{s.label}</Link>}
          {s.label && s.release_date && ' · '}{s.release_date && <span>{s.release_date.slice(0, 4)}</span>}
        </div>
      )}
      <div className="text-sm text-muted mt-0.5 truncate">
        {humanCount(Math.round(10 ** s.logcount))} playlists
        <DebugId id={s.album_rowid}/>
      </div>
    </>
  )
}

function ArtistPanel({s, navigate}: {s: ArtistInfo; navigate: NavigateFn}) {
  return (
    <>
      <div className="mb-2"><Badge entityType="artist"/></div>
      <div className="text-lg font-semibold leading-snug truncate">{s.artist_name}</div>
      <div className="text-sm text-muted mt-0.5 truncate">
        {s.artist_genre && <>{s.artist_genre} · </>}
        {humanCount(Math.round(10 ** s.logcount))} playlists
        <DebugId id={s.artist_rowid}/>
      </div>
      {s.reprs?.length > 0 && (
        <ReprRow>
          <span className="text-muted mr-1.5">top albums:</span>
          {s.reprs.map((r, i) => (
            <span key={r.album_rowid}>
              {i > 0 && <span className="text-muted mx-1">·</span>}
              <Link onClick={() => navigate('album', r.album_rowid, r.lon, r.lat)} color="var(--color-album)">{r.album_name_norm}</Link>
            </span>
          ))}
        </ReprRow>
      )}
    </>
  )
}

function LabelPanel({s, navigate}: {s: LabelInfo; navigate: NavigateFn}) {
  return (
    <>
      <div className="mb-2"><Badge entityType="label"/></div>
      <div className="text-lg font-semibold leading-snug truncate">{s.label}</div>
      <div className="text-sm text-muted mt-0.5 truncate">
        {humanCount(Math.round(10 ** s.logcount))} playlists
        <DebugId id={s.label_rowid}/>
      </div>
      {s.reprs?.length > 0 && (
        <ReprRow>
          <span className="text-muted mr-1.5">top artists:</span>
          {s.reprs.map((r, i) => (
            <span key={r.artist_rowid}>
              {i > 0 && <span className="text-muted mx-1">·</span>}
              <Link onClick={() => navigate('artist', r.artist_rowid, r.lon, r.lat)} color="var(--color-artist)">{r.artist_name}</Link>
            </span>
          ))}
        </ReprRow>
      )}
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

/** Returns display name and subtitle for a recommendation. */
function getRecDisplay(rec: Recommend, entityType: string): {name: string; sub: string} {
  const playlists = humanCount(Math.round(10 ** rec.logcount)) + ' playlists'
  switch (entityType) {
    case 'track':  return {name: (rec as TrackRecommend).track_name_norm, sub: (rec as TrackRecommend).artist_name + ' · ' + playlists}
    case 'album':  return {name: (rec as AlbumRecommend).album_name_norm, sub: (rec as AlbumRecommend).artist_name + ' · ' + playlists}
    case 'artist': {
      const r = rec as ArtistRecommend
      const parts = [r.artist_genre, playlists].filter(Boolean)
      return {name: r.artist_name, sub: parts.join(' · ')}
    }
    default:       return {name: (rec as LabelRecommend).label, sub: playlists}
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
        <div className="text-xs text-muted truncate">{sub}</div>
      </button>
    </li>
  )
}

/** Renders loading/error/empty/list states for recommendations. */
function RecBody({recs, entityType, navigate}: {recs: RecsState | null; entityType: string; navigate: NavigateFn}) {
  if (!recs || recs.status === 'loading')
    return <div className="text-muted text-xs py-2 animate-pulse px-4">Loading...</div>
  if (recs.status === 'error')
    return <div className="text-muted text-xs py-2 px-4">Failed to load.</div>
  if (recs.items.length === 0)
    return <div className="text-muted text-xs py-2 px-4">No recommendations.</div>
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
  const RECSNUMBER = 10

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
      {open && (
        <div className="max-h-40 overflow-y-auto overscroll-contain no-scrollbar">
          <RecBody recs={recs} entityType={entity.entity_type} navigate={navigate}/>
        </div>
      )}
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
    body = <TrackPanel s={selection} navigate={navigate}/>
  } else if (selection.entity_type === 'album') {
    body = <AlbumPanel s={selection} navigate={navigate}/>
  } else if (selection.entity_type === 'artist') {
    body = <ArtistPanel s={selection} navigate={navigate}/>
  } else {
    body = <LabelPanel s={selection} navigate={navigate}/>
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
      <div className="relative px-4 pt-4">
        {body}
        <button
          onClick={onClose}
          className="absolute top-0 right-0 text-muted hover:text-white transition-colors text-lg leading-none p-4"
          aria-label="Close"
        >×</button>
      </div>
      {selection.status === 'loaded' && (
        <RecsSection key={`${selection.entity_type}:${getRowid(selection)}`} entity={selection} navigate={navigate}/>
      )}
    </div>
  )
}
