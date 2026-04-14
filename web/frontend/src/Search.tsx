import {useEffect, useId, useRef, useState} from 'react'
import {AlbumSummary, ArtistSummary, LabelSummary, TrackSummary} from './Summary.tsx'
import {type EntityType, formatPlaylistCount} from './utils.ts'
import {makeAbortable} from './requests.ts'
import {getRowid} from './utils.ts'


// --- Search types mirroring backend TypedDicts ---

type TrackHit = {
  entity_type: 'track'
  track_rowid: number
  track_name_norm: string
  artist_name: string
  lon: number
  lat: number
  logcount: number
}

type AlbumHit = {
  entity_type: 'album'
  album_rowid: number
  album_name_norm: string
  artist_name: string
  lon: number
  lat: number
  logcount: number
}

type ArtistHit = {
  entity_type: 'artist'
  artist_rowid: number
  artist_name: string
  lon: number
  lat: number
  logcount: number
}

type LabelHit = {
  entity_type: 'label'
  label_rowid: number
  label: string
  lon: number
  lat: number
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
      return <TrackSummary trackName={hit.track_name_norm} artist={hit.artist_name}/>
    case 'album':
      return <AlbumSummary albumName={hit.album_name_norm} artist={hit.artist_name}/>
    case 'artist':
      return (
        <ArtistSummary
          artistName={hit.artist_name}
          playlistCount={formatPlaylistCount(hit.logcount)}
        />
      )
    case 'label':
      return (
        <LabelSummary
          labelName={hit.label}
          playlistCount={formatPlaylistCount(hit.logcount)}
        />
      )
  }
}

type SearchDropdownProps = {
  listboxId: string
  results: SearchHit[]
  open: boolean
  activeIdx: number | null
  getOptionId: (hit: SearchHit) => string
  onActivate: (index: number | null) => void
  onSelect: (hit: SearchHit) => void
}

/** Local dropdown component for search result list behavior and rendering only. */
function SearchDropdown({
  listboxId,
  results,
  open,
  activeIdx,
  getOptionId,
  onActivate,
  onSelect,
}: SearchDropdownProps) {
  const activeItemRef = useRef<HTMLLIElement>(null)

  useEffect(() => {
    activeItemRef.current?.scrollIntoView({block: 'nearest'})
  }, [activeIdx])

  if (!open || results.length === 0) return null

  return (
    <div
      className="bg-surface rounded-xl mt-1 max-h-[30dvh] flex flex-col pt-4"
      style={{paddingBottom: 'calc(1.25rem + env(safe-area-inset-bottom))'}}
    >
      <ul
        id={listboxId}
        role="listbox"
        className="min-h-0 overflow-y-auto overscroll-contain no-scrollbar list-none"
      >
        {results.map((hit, i) => (
          <li
            id={getOptionId(hit)}
            role="option"
            aria-selected={i === activeIdx}
            key={`${hit.entity_type}-${getRowid(hit)}`}
            ref={i === activeIdx ? activeItemRef : null}
            onMouseDown={(e) => e.preventDefault()}
            onClick={() => onSelect(hit)}
            onMouseEnter={() => onActivate(i)}
            onMouseLeave={() => onActivate(null)}
            className={`cursor-pointer px-4 py-2 ${i === activeIdx ? 'bg-overlay' : ''}`}
          >
            {getSummary(hit)}
          </li>
        ))}
      </ul>
    </div>
  )
}

/** Search input with debounced API calls, a results dropdown, and keyboard navigation. */
export default function Search({navigate}: SearchProps) {
  const [query, setQuery] = useState('')
  const [results, setResults] = useState<SearchHit[]>([])
  const [open, setOpen] = useState(false)
  const [activeIdx, setActiveIdx] = useState<number | null>(null)
  const nextSearch = useRef(makeAbortable())
  const containerRef = useRef<HTMLDivElement>(null)
  const listboxId = useId()

  // Debounced search fetch — fires 300ms after the query stops changing.
  const DEBOUNCE_MS = 300
  useEffect(() => {
    if (!query.trim()) {
      nextSearch.current.cancel()
      return
    }
    const timer = setTimeout(() => {
      const signal = nextSearch.current.nextSignal()
      fetch(`/api/search?q=${encodeURIComponent(query)}&limit=10`, {signal})
        .then(r => {
          if (!r.ok) throw new Error(r.statusText)
          return r.json()
        })
        .then((hits: SearchHit[]) => {
          setResults(hits)
          setOpen(true)
          setActiveIdx(null)
        })
        .catch(err => {
          if (err.name !== 'AbortError') setResults([])
        })
    }, DEBOUNCE_MS)
    return () => clearTimeout(timer)
  }, [query])

  /** Clears the query, results, and keyboard selection. */
  function clearSearch() {
    nextSearch.current.cancel()
    setQuery('')
    setResults([])
    setOpen(false)
    setActiveIdx(null)
  }

  /** Navigates to a hit and closes the dropdown. */
  function handleSelect(hit: SearchHit) {
    navigate(hit.entity_type, getRowid(hit), hit.lon, hit.lat)
    setResults([])
    setOpen(false)
    setActiveIdx(null)
  }

  function getOptionId(hit: SearchHit) {
    return `${listboxId}-${hit.entity_type}-${getRowid(hit)}`
  }

  /** Handles arrow key navigation, Enter to select, and Escape to clear. */
  function handleKeyDown(e: React.KeyboardEvent) {
    if (!open || results.length === 0) {
      if (e.key === 'Escape') clearSearch()
      return
    }

    if (e.key === 'Escape') {
      clearSearch()
      return
    }

    if (e.key === 'ArrowDown') {
      setActiveIdx(index => (index == null ? 0 : Math.min(index + 1, results.length - 1)))
      e.preventDefault()
      return
    }
    if (e.key === 'ArrowUp') {
      setActiveIdx(index => (index == null ? results.length - 1 : Math.max(index - 1, 0)))
      e.preventDefault()
      return
    }
    if (e.key === 'Enter' && activeIdx != null) {
      handleSelect(results[activeIdx])
    }
  }

  function handleBlur(e: React.FocusEvent<HTMLDivElement>) {
    const nextFocused = e.relatedTarget
    if (nextFocused instanceof Node && containerRef.current?.contains(nextFocused)) {
      return
    }
    setOpen(false)
    setActiveIdx(null)
  }

  const activeDescendant =
    open && activeIdx != null && results[activeIdx] ? getOptionId(results[activeIdx]) : undefined

  return (
    <div
      ref={containerRef}
      // touch-auto re-enables touch interactions with the search UI elements
      className="
      absolute top-3 z-100 w-[90%] left-1/2 -translate-x-1/2 sm:w-md sm:left-3 sm:translate-x-0
      touch-auto
      "
      onBlur={handleBlur}
      // these will prevent UI interaction to bubble up to the map
      onPointerDown={(e) => e.stopPropagation()}
      onPointerMove={(e) => e.stopPropagation()}
    >
      <div className="flex items-center bg-surface rounded-3xl h-12">
        <input
          role="combobox"
          aria-autocomplete="list"
          aria-expanded={query.trim() !== '' && open && results.length > 0}
          aria-controls={listboxId}
          aria-activedescendant={activeDescendant}
          value={query}
          onChange={(e) => {
            setQuery(e.target.value)
            setActiveIdx(null)
            if (!e.target.value.trim()) setOpen(false)
          }}
          onFocus={() => setOpen(true)}
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
      <SearchDropdown
        listboxId={listboxId}
        results={results}
        open={query.trim() !== '' && open}
        activeIdx={activeIdx}
        getOptionId={getOptionId}
        onActivate={setActiveIdx}
        onSelect={handleSelect}
      />
    </div>
  )
}
