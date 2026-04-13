import {useCallback, useEffect, useRef, useState} from 'react'
import maplibregl from 'maplibre-gl'
import 'maplibre-gl/dist/maplibre-gl.css'
import type {FeatureCollection} from 'geojson'
import Search from './Search.tsx'
import type {Selection, UpdateFn} from './Panel.tsx'
import Panel from './Panel.tsx'
import {makeAbortable} from './requests.ts'
import {getRowid} from './utils.ts'
import earwaxLogo from './assets/earwax_simple.svg'


const MAX_HISTORY = 20

const INITIAL_VIEW = {
  center: [9.93, -4.64] as [number, number],
  zoom: 6,
  minZoom: 5,
  maxZoom: 14,
  pitch: 10,
}

// MapLibre requires absolute URLs for tile sources. In dev the env var is a relative path
// and we prepend the page origin so it works on both localhost and LAN.
// TODO: set VITE_TILES_URL to the full CDN URL in production (e.g. https://cdn.example.com/tiles/{z}/{x}/{y}.pbf)
const TILE_URL_RAW = import.meta.env.VITE_TILES_URL as string
const TILE_URL = TILE_URL_RAW.startsWith('http') ? TILE_URL_RAW : window.location.origin + TILE_URL_RAW

/** Reads a CSS custom property value (e.g. "#3bda28") from :root. */
function cssVar(name: string): string {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim()
}

/** 5-degree lat/lon graticule rendered below entity layers. */
const GRID_LINES: FeatureCollection = {type: 'FeatureCollection', features: []}
for (let lon = -180; lon <= 180; lon += 5) {
  GRID_LINES.features.push({
    type: 'Feature',
    geometry: {type: 'LineString', coordinates: [[lon, -90], [lon, 90]]},
    properties: {},
  })
}
for (let lat = -90; lat <= 90; lat += 5) {
  GRID_LINES.features.push({
    type: 'Feature',
    geometry: {type: 'LineString', coordinates: [[-180, lat], [180, lat]]},
    properties: {},
  })
}


/** Reads map state from the URL hash. Missing keys fall back to INITIAL_VIEW defaults. */
function parseHash() {
  const p = new URLSearchParams(window.location.hash.slice(1))
  const lon = parseFloat(p.get('lon') ?? '')
  const lat = parseFloat(p.get('lat') ?? '')
  const z = parseFloat(p.get('z') ?? '')
  return {
    center: [
      isNaN(lon) ? INITIAL_VIEW.center[0] : lon,
      isNaN(lat) ? INITIAL_VIEW.center[1] : lat,
    ] as [number, number],
    zoom: isNaN(z) ? INITIAL_VIEW.zoom : z,
    entityType: p.get('entity'),
    rowid: p.get('rowid'),
  }
}

/** Merges updates into the current URL hash. */
function updateHash(updates: Record<string, string | number | null>) {
  const p = new URLSearchParams(window.location.hash.slice(1))
  for (const [k, v] of Object.entries(updates))
    if (v == null) p.delete(k)
    else p.set(k, String(v))
  history.replaceState(null, '', '#' + p.toString())
}


/** Fetches entity info from the API, validating the response status. */
function fetchEntityInfo(entityType: string, rowid: number | string, signal: AbortSignal) {
  return fetch(`/api/panel?rowid=${rowid}&entity_name=${entityType}`, {signal})
    .then(r => {
      if (!r.ok) throw new Error(r.statusText);
      return r.json()
    })
}

/** Root application component — owns view state, selection, and wires navigation to the map. */
export default function App() {
  const containerRef = useRef<HTMLDivElement>(null)
  const mapRef = useRef<maplibregl.Map | null>(null)
  const [initHash] = useState(parseHash)

  // Navigation stack: the last element is the panel's current selection, earlier
  // elements are the back-button breadcrumb. Only one loading/error entry can
  // ever be on top — clicks during an unresolved fetch replace, not append.
  const [stack, setStack] = useState<Selection[]>(() =>
    initHash.entityType && initHash.rowid
      ? [{status: 'loading'}]
      : []
  )
  const nextSelection = useRef(makeAbortable())
  const current = stack.length > 0 ? stack[stack.length - 1] : null

  /** Fetches entity info and pushes it onto the nav stack, without moving the map. */
  const push = useCallback((entityType: string, rowid: number) => {
    setStack(prev => {
      const pending: Selection = {status: 'loading'}
      const top = prev[prev.length - 1]
      // Only loaded entries extend history; an unresolved top is replaced in place.
      if (!top || top.status === 'loaded')
        return [...prev.slice(-(MAX_HISTORY - 1)), pending]
      return [...prev.slice(0, -1), pending]
    })
    updateHash({entity: entityType, rowid})
    const signal = nextSelection.current.nextSignal()
    fetchEntityInfo(entityType, rowid, signal)
      .then(data => setStack(prev => [
        ...prev.slice(0, -1),
        {status: 'loaded', entity_type: entityType, ...data}
      ]))
      .catch(err => {
        if (err.name !== 'AbortError') setStack(prev => [...prev.slice(0, -1), {
          status: 'error',
          entity_type: entityType,
          rowid
        }])
      })
  }, [])

  /** Shallow-merges a patch into the current top entry when it still matches the target entity. */
  const update = useCallback<UpdateFn>((entityType, rowid, patch) => {
    setStack(prev => {
      const i = prev.length - 1
      if (i < 0) return prev
      const current = prev[i]
      if (current.status !== 'loaded') return prev
      // the next two lines protect against a race in which:
      //  1. recommends are fetched for A
      //  2. B is pushed top of the stack
      //  3. A's recommend are erroneously attached to B
      if (current.entity_type !== entityType) return prev
      if (getRowid(current) !== rowid) return prev
      return [...prev.slice(0, i), {...current, ...patch} as Selection]
    })
  }, [])

  /** Flies the map to the given coordinates and pushes a new selection. */
  const navigate = useCallback((entityType: string, rowid: number, lon: number, lat: number) => {
    mapRef.current?.flyTo({center: [lon, lat]})
    push(entityType, rowid)
  }, [push])

  /** Pops the top entry and flies back to the one beneath it (already loaded, no refetch). */
  function pop() {
    setStack(prev => {
      if (prev.length < 2) return prev
      // invariant: only `loaded` info can get under top of the stack.
      // guaranteed by `push`
      const next = prev[prev.length - 2] as Extract<Selection, { status: 'loaded' }>
      updateHash({entity: next.entity_type, rowid: getRowid(next)})
      mapRef.current?.flyTo({center: [next.lon, next.lat], zoom: 8})
      return prev.slice(0, -1)
    })
  }

  /** Closes the panel and clears the nav stack. */
  function handlePanelClose() {
    nextSelection.current.cancel()
    setStack([])
    updateHash({entity: null, rowid: null})
  }

  /** Fetches entity info from URL hash on initial mount, without moving the map. */
  useEffect(() => {
    const {entityType, rowid} = initHash
    if (!entityType || !rowid) return
    push(entityType, Number(rowid))
  }, [])

  /** Initializes the MapLibre map, adds layers, and wires interaction handlers. */
  useEffect(() => {
    const map = new maplibregl.Map({
      container: containerRef.current!,
      style: {
        version: 8,
        sources: {},
        layers: [{id: 'background', type: 'background', paint: {'background-color': '#0d0d12'}}],
      },
      center: initHash.center,
      zoom: initHash.zoom,
      minZoom: INITIAL_VIEW.minZoom,
      maxZoom: INITIAL_VIEW.maxZoom,
      pitch: INITIAL_VIEW.pitch,
      renderWorldCopies: false,
      attributionControl: false,
    })
    mapRef.current = map

    map.on('load', () => {
      // Grid layer — below all entity layers
      map.addSource('grid', {type: 'geojson', data: GRID_LINES})
      map.addLayer({
        id: 'grid',
        type: 'line',
        source: 'grid',
        paint: {
          'line-color': cssVar('--color-border'),
          'line-width': 1,
        },
      })

      // Single vector source carrying all four entity layers from the tileset
      map.addSource('entities', {
        type: 'vector',
        tiles: [TILE_URL],
        minzoom: INITIAL_VIEW.minZoom,
        maxzoom: INITIAL_VIEW.maxZoom,
      })

      map.addLayer({
        id: 'labels', type: 'circle', source: 'entities', 'source-layer': 'labels',
        paint: {
          'circle-radius': ['max', 1, ['get', 'logcount']],
          'circle-color': 'transparent',
          'circle-stroke-color': cssVar('--color-label'),
          'circle-stroke-opacity': 0.7,
          'circle-stroke-width': 1,
        },
      })
      map.addLayer({
        id: 'albums', type: 'circle', source: 'entities', 'source-layer': 'albums',
        paint: {
          'circle-radius': ['max', 1, ['get', 'logcount']],
          'circle-color': cssVar('--color-album'),
          'circle-opacity': 0.7,
        },
      })
      map.addLayer({
        id: 'artists', type: 'circle', source: 'entities', 'source-layer': 'artists',
        paint: {
          'circle-radius': ['max', 1, ['get', 'logcount']],
          'circle-color': 'transparent',
          'circle-stroke-color': cssVar('--color-artist'),
          'circle-stroke-opacity': 0.7,
          'circle-stroke-width': 1,
        },
      })
      map.addLayer({
        id: 'tracks', type: 'circle', source: 'entities', 'source-layer': 'tracks',
        paint: {
          'circle-radius': ['max', 1, ['get', 'logcount']],
          'circle-color': cssVar('--color-track'),
          'circle-opacity': 0.7,
        },
      })


      // Click handlers — each layer dispatches to push with its rowid property
      map.on('click', 'tracks', (e) => {
        const rowid = e.features?.[0]?.properties.track_rowid
        push('track', rowid)
      })
      map.on('click', 'albums', (e) => {
        const rowid = e.features?.[0]?.properties.album_rowid
        push('album', rowid)
      })
      map.on('click', 'artists', (e) => {
        const rowid = e.features?.[0]?.properties.artist_rowid
        push('artist', rowid)
      })
      map.on('click', 'labels', (e) => {
        const rowid = e.features?.[0]?.properties.label_rowid
        push('label', rowid)
      })

      // Pointer cursor on hover for all pickable layers
      for (const id of ['tracks', 'albums', 'artists', 'labels']) {
        map.on('mouseenter', id, () => {
          map.getCanvas().style.cursor = 'pointer'
        })
        map.on('mouseleave', id, () => {
          map.getCanvas().style.cursor = ''
        })
      }
    })

    /** Debounced URL hash sync — avoids iOS Safari replaceState rate-limit during flyTo. */
    let hashTimer: ReturnType<typeof setTimeout> | null = null
    map.on('moveend', () => {
      if (hashTimer) clearTimeout(hashTimer)
      hashTimer = setTimeout(() => {
        const c = map.getCenter()
        updateHash({
          lon: c.lng.toFixed(4),
          lat: c.lat.toFixed(4),
          z: map.getZoom().toFixed(2),
        })
        hashTimer = null
      }, 100)
    })

    return () => map.remove()
  }, [push])

  return (
    <div className="relative w-screen h-screen">
      <div ref={containerRef} className="w-full h-full"/>
      <Search navigate={navigate}/>
      <Panel
        selection={current}
        navigate={navigate}
        update={update}
        onClose={handlePanelClose}
        goBack={stack.length > 1 ? pop : null}
      />
      <img
        src={earwaxLogo}
        alt="earwax"
        className="fixed z-5 sm:w-28 w-16 pointer-events-none
                   bottom-4 right-4"
      />
    </div>
  )
}
