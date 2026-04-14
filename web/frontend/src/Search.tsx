import {useEffect, useRef, useState} from 'react'
import {AlbumSummary, ArtistSummary, LabelSummary, TrackSummary} from './Summary.tsx'
import {type EntityType, formatPlaylistCount} from './utils.ts'
import {makeAbortable} from "./requests.ts";
import {getRowid} from "./utils.ts";


// --- Search types mirroring backend TypedDicts ---

type TrackHit = {
  entity_type: 'track';
  track_rowid: number;
  track_name_norm: string;
  artist_name: string;
  lon: number;
  lat: number;
  logcount: number
}

type AlbumHit = {
  entity_type: 'album';
  album_rowid: number;
  album_name_norm: string;
  artist_name: string;
  lon: number;
  lat: number;
  logcount: number
}

type ArtistHit = {
  entity_type: 'artist';
  artist_rowid: number;
  artist_name: string;
  lon: number;
  lat: number;
  logcount: number
}

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
  navigate: (entityType: EntityType, rowid: number, lon: number, lat: number) => void
}

/** Returns the rendered summary component for a hit. */
function getSummary(hit: SearchHit) {
  switch (hit.entity_type) {
    case 'track':
      return <TrackSummary
        trackName={hit.track_name_norm}
        artist={hit.artist_name}
      />
    case 'album':
      return <AlbumSummary
        albumName={hit.album_name_norm}
        artist={hit.artist_name}
      />
    case 'artist':
      return <ArtistSummary
        artistName={hit.artist_name}
        playlistCount={formatPlaylistCount(hit.logcount)}
      />
    case 'label':
      return <LabelSummary
        labelName={hit.label}
        playlistCount={formatPlaylistCount(hit.logcount)}
      />
  }
}


/** Search input with debounced API calls, a results dropdown, and keyboard navigation. */
export default function Search({navigate}: SearchProps) {
  const [query, setQuery] = useState('')
  const [results, setResults] = useState<SearchHit[]>([])
  const [open, setOpen] = useState(false)
  const [activeIdx, setActiveIdx] = useState<number | null>(null)
  const activeItemRef = useRef<HTMLLIElement>(null)
  const nextSearch = useRef(makeAbortable())

  // Scroll the active item into view when keyboard navigation changes it.
  useEffect(() => {
    activeItemRef.current?.scrollIntoView({block: 'nearest'})
  }, [activeIdx])

  // Debounced search fetch — fires 300ms after the query stops changing.
  const DEBOUNCE_MS = 300;
  useEffect(() => {
    if (!query.trim()) {
      nextSearch.current.cancel();
      return
    }
    const timer = setTimeout(() => {
      const signal = nextSearch.current.nextSignal()
      fetch(`/api/search?q=${encodeURIComponent(query)}&limit=10`, {signal})
        .then(r => { if (!r.ok) throw new Error(r.statusText); return r.json() })
        .then((hits: SearchHit[]) => {
          setResults(hits)
          setOpen(true)
          setActiveIdx(null)
        })
        .catch(err => { if (err.name !== 'AbortError') setResults([]) })
    }, DEBOUNCE_MS)
    return () => clearTimeout(timer)
  }, [query])

  /** Clears the query, results, and keyboard selection. */
  function clearSearch() {
    nextSearch.current.cancel()
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
      absolute top-3 z-100 w-[90%] left-1/2 -translate-x-1/2 sm:w-md sm:left-3 sm:translate-x-0
      touch-auto
      "
      // these will prevent UI interaction to bubble up to the map
      onPointerDown={(e) => e.stopPropagation()}
      onPointerMove={(e) => e.stopPropagation()}
    >
      <div className="flex items-center bg-surface rounded-3xl h-12">
        <input
          value={query}
          onChange={(e) => { setQuery(e.target.value); if (!e.target.value.trim()) setOpen(false) }}
          onFocus={() => setOpen(true)}
          onBlur={() => setTimeout(() => setOpen(false), 150)}
          onKeyDown={handleKeyDown}
          placeholder="search…"
          className="flex-1 bg-transparent border-0 outline-none text-white placeholder:text-muted pl-4 pr-1"
        />
        {query && (
          <button
            type="button"
            onClick={clearSearch}
            aria-label="Clear search"
            className="text-muted hover:text-white text-lg leading-none cursor-pointer px-4 py-3"
          >×</button>
        )}
      </div>
      {query.trim() && results.length > 0 && open && (
        <div className="bg-surface rounded-xl mt-1 max-h-[30dvh] flex flex-col pt-4"
             style={{paddingBottom: 'calc(1.25rem + env(safe-area-inset-bottom))'}}>
          <ul className="min-h-0 overflow-y-auto overscroll-contain no-scrollbar list-none">
            {results.map((hit, i) => {
              const summary = getSummary(hit)
              return (
                <li
                  key={`${hit.entity_type}-${getRowid(hit)}`}
                  ref={i === activeIdx ? activeItemRef : null}
                  onClick={() => handleSelect(hit)}
                  onMouseEnter={() => setActiveIdx(i)}
                  onMouseLeave={() => setActiveIdx(null)}
                  className={`cursor-pointer px-4 py-2 ${i === activeIdx ? 'bg-overlay' : ''}`}
                >
                  {summary}
                </li>
              )
            })}
          </ul>
        </div>
      )}
    </div>
  )
}
