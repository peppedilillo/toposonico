import {useEffect, useRef, useState} from 'react'
import Badge from './Badge'

/** A track search result. */
type TrackHit = {
  entity_type: 'track';
  track_rowid: number;
  track_name: string;
  artist_name: string;
  lon: number;
  lat: number;
  logcount: number
}

/** An album search result. */
type AlbumHit = {
  entity_type: 'album';
  album_rowid: number;
  album_name_norm: string;
  artist_name: string;
  lon: number;
  lat: number;
  logcount: number
}

/** An artist search result. */
type ArtistHit = {
  entity_type: 'artist';
  artist_rowid: number;
  artist_name: string;
  lon: number;
  lat: number;
  logcount: number
}

/** A label search result. */
type LabelHit = {
  entity_type: 'label';
  label_rowid: number;
  label: string;
  lon: number;
  lat: number;
  logcount: number
}

/** A single result from the search API. */
type SearchHit = TrackHit | AlbumHit | ArtistHit | LabelHit

/** Props for the Search component. */
type SearchProps = {
  navigate: (entityType: string, rowid: number, lon: number, lat: number) => void
}

/** Extracts the numeric rowid, narrowing by entity_type. */
function getRowid(hit: SearchHit): number {
  switch (hit.entity_type) {
    case 'track':
      return hit.track_rowid
    case 'album':
      return hit.album_rowid
    case 'artist':
      return hit.artist_rowid
    case 'label':
      return hit.label_rowid
  }
}

/** Returns the primary display name, narrowing by entity_type. */
function getName(hit: SearchHit): string {
  switch (hit.entity_type) {
    case 'track':
      return hit.track_name
    case 'album':
      return hit.album_name_norm
    case 'artist':
      return hit.artist_name
    case 'label':
      return hit.label
  }
}

/** Returns the secondary line for a hit, or null if none applies. */
function getSubtitle(hit: SearchHit): string | null {
  switch (hit.entity_type) {
    case 'track':
      return hit.artist_name
    case 'album':
      return hit.artist_name
    case 'artist':
      return null
    case 'label':
      return null
  }
}

/** Search input with debounced API calls, a results dropdown, and keyboard navigation. */
export default function Search({navigate}: SearchProps) {
  const [query, setQuery] = useState('')
  const [results, setResults] = useState<SearchHit[]>([])
  const [open, setOpen] = useState(false)
  const [activeIdx, setActiveIdx] = useState<number | null>(null)
  const activeItemRef = useRef<HTMLLIElement>(null)

  // Scroll the active item into view when keyboard navigation changes it.
  useEffect(() => {
    activeItemRef.current?.scrollIntoView({block: 'nearest'})
  }, [activeIdx])

  // Debounced search fetch — fires 300ms after the query stops changing.
  const DEBOUNCE_MS = 300;
  useEffect(() => {
    if (!query.trim()) {
      setResults([])
      setActiveIdx(null)
      return
    }
    const timer = setTimeout(() => {
      fetch(`/api/search?q=${encodeURIComponent(query)}&limit=10`)
        .then(r => r.json())
        .then((hits: SearchHit[]) => {
          setResults(hits)
          setOpen(true)
          setActiveIdx(null)
        })
        .catch(() => setResults([]))
    }, DEBOUNCE_MS)
    return () => clearTimeout(timer)
  }, [query])

  /** Clears the query, results, and keyboard selection. */
  function clearSearch() {
    setQuery('')
    setResults([])
    setActiveIdx(null)
  }

  /** Navigates to a hit and closes the dropdown. */
  function handleSelect(hit: SearchHit) {
    navigate(hit.entity_type, getRowid(hit), hit.lon, hit.lat)
    setResults([])
    setOpen(false)
  }

  /** Handles arrow key navigation, Enter to select, and Escape to clear. */
  function handleKeyDown(e: React.KeyboardEvent) {
    if (e.key === 'ArrowDown') {
      setActiveIdx(i => i === null ? 0 : Math.min(i + 1, results.length - 1))
    } else if (e.key === 'ArrowUp') {
      setActiveIdx(i => i === null ? results.length - 1 : Math.max(i - 1, 0))
    } else if (e.key === 'Enter' && activeIdx !== null) {
      handleSelect(results[activeIdx])
    } else if (e.key === 'Escape') {
      clearSearch()
    }
  }

  return (
    <div
      // touch-auto re-enables touch interactions with the search UI elements
      className="
      absolute top-3 z-100 w-[90%] left-1/2 -translate-x-1/2 sm:w-80 sm:left-3 sm:translate-x-0
      touch-auto
      "
      // these will prevent UI interaction to bubble up to the map
      onPointerDown={(e) => e.stopPropagation()}
      onPointerMove={(e) => e.stopPropagation()}
    >
      <div className="flex items-center bg-surface rounded-3xl h-12">
        <input
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onFocus={() => setOpen(true)}
          onBlur={() => setTimeout(() => setOpen(false), 150)}
          onKeyDown={handleKeyDown}
          placeholder="search…"
          className="flex-1 bg-transparent border-0 outline-none text-white placeholder:text-muted pl-4 pr-1"
        />
        {query && (
          <div
            onClick={clearSearch}
            aria-label="Clear search"
            role="button"
            tabIndex={-1}
            className="text-muted hover:text-white text-lg leading-none cursor-pointer px-4 py-3"
          >×</div>
        )}
      </div>
      {results.length > 0 && open && (
        <ul
          className="bg-surface rounded-xl mt-1 py-2 max-h-[30dvh] overflow-y-auto overscroll-contain list-none">
          {results.map((hit, i) => {
            const subtitle = getSubtitle(hit)
            return (
              <li
                key={`${hit.entity_type}-${getRowid(hit)}`}
                ref={i === activeIdx ? activeItemRef : null}
                onClick={() => handleSelect(hit)}
                onMouseEnter={() => setActiveIdx(i)}
                onMouseLeave={() => setActiveIdx(null)}
                className={`cursor-pointer px-4 py-2 ${i === activeIdx ? 'bg-overlay' : ''}`}
              >
                <div className="font-medium text-white truncate">{getName(hit)}</div>
                {subtitle && <div className="text-sm text-muted truncate">{subtitle}</div>}
                <Badge entityType={hit.entity_type}/>
              </li>
            )
          })}
        </ul>
      )}
    </div>
  )
}
