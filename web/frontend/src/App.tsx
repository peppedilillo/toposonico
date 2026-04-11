import {useCallback, useEffect, useRef, useState} from 'react'
import Search from './Search'
import type {Selection} from './Panel'
import Panel from './Panel'
import type {MapViewState, PickingInfo} from '@deck.gl/core'
import {FlyToInterpolator, MapView} from '@deck.gl/core'
import {MVTLayer} from '@deck.gl/geo-layers'
import {GeoJsonLayer} from '@deck.gl/layers'
import DeckGL from '@deck.gl/react'
import type {FeatureCollection} from 'geojson'
import {makeAbortable} from "./requests.ts";
import {getRowid} from './utils.ts'
import earwaxLogo from './assets/earwax.svg'


const MAX_HISTORY = 20

const INITIAL_VIEW_STATE: MapViewState = {
  longitude: 9.93,
  latitude: -4.64,
  zoom: 6,
  pitch: 10,
  bearing: 0,
  minZoom: 5,
  maxZoom: 14,
}

const TILES = '/tiles/{z}/{x}/{y}.pbf'


/** Reads a CSS custom property and returns a deck.gl-compatible [r, g, b, a] color array. */
function cssColor(variable: string, alpha = 255): [number, number, number, number] {
  const hex = getComputedStyle(document.documentElement).getPropertyValue(variable).trim()
  const n = parseInt(hex.slice(1), 16)
  return [(n >> 16) & 255, (n >> 8) & 255, n & 255, alpha]
}

/** Entity colors. Needed as we can't read CSS from layers. **/
const COLORS = {
  border: cssColor('--color-border'),
  track: cssColor('--color-track', 180),
  album: cssColor('--color-album', 180),
  artist: cssColor('--color-artist', 180),
  label: cssColor('--color-label', 180),
}


/** Reads map state from the URL hash. Missing keys fall back to INITIAL_VIEW_STATE defaults. */
function parseHash() {
  const p = new URLSearchParams(window.location.hash.slice(1))
  const lon = parseFloat(p.get('lon') ?? '')
  const lat = parseFloat(p.get('lat') ?? '')
  const z = parseFloat(p.get('z') ?? '')
  return {
    longitude: isNaN(lon) ? INITIAL_VIEW_STATE.longitude : lon,
    latitude: isNaN(lat) ? INITIAL_VIEW_STATE.latitude : lat,
    zoom: isNaN(z) ? INITIAL_VIEW_STATE.zoom : z,
    entity: p.get('entity'),
    rowid: p.get('rowid'),
  }
}

/** Merges updates into the current URL. **/
function updateHash(updates: Record<string, string | number | null>) {
  const p = new URLSearchParams(window.location.hash.slice(1))
  for (const [k, v] of Object.entries(updates))
    if (v == null) p.delete(k)
    else p.set(k, String(v))
  history.replaceState(null, '', '#' + p.toString())
}


/** Renders the lat/lon graticule as a GeoJSON line layer. */
const gridLines: FeatureCollection = {type: 'FeatureCollection', features: []}
for (let lon = -180; lon <= 180; lon += 5) {
  gridLines.features.push({
    type: 'Feature',
    geometry: {type: 'LineString', coordinates: [[lon, -90], [lon, 90]]},
    properties: {},
  })
}
for (let lat = -90; lat <= 90; lat += 5) {
  gridLines.features.push({
    type: 'Feature',
    geometry: {type: 'LineString', coordinates: [[-180, lat], [180, lat]]},
    properties: {},
  })
}

const gridLayer = new GeoJsonLayer({
  id: 'grid',
  data: gridLines,
  getLineColor: COLORS.border,
  getLineWidth: 1,
  lineWidthUnits: 'pixels',
})

/** Maps a tile feature's logcount to a pixel radius. */
function getPointRadius(feature: { properties: { logcount: number } }) {
  return feature.properties.logcount
}

/** MVT layers — one per entity type. */
const tracksLayer = new MVTLayer({
  id: 'tracks',
  data: TILES,
  loadOptions: {mvt: {layers: ['tracks']}},
  pointType: 'circle',
  getPointRadius: getPointRadius,
  pointRadiusUnits: 'pixels',
  pointRadiusMinPixels: 1,
  getFillColor: COLORS.track,
  pickable: true,
})

const albumsLayer = new MVTLayer({
  id: 'albums',
  data: TILES,
  loadOptions: {mvt: {layers: ['albums']}},
  pointType: 'circle',
  getPointRadius: getPointRadius,
  pointRadiusUnits: 'pixels',
  pointRadiusMinPixels: 1,
  getFillColor: COLORS.album,
  pickable: true,
})

const artistsLayer = new MVTLayer({
  id: 'artists',
  data: TILES,
  loadOptions: {mvt: {layers: ['artists']}},
  pointType: 'circle',
  getPointRadius: getPointRadius,
  pointRadiusUnits: 'pixels',
  pointRadiusMinPixels: 1,
  filled: false,
  stroked: true,
  getLineColor: COLORS.artist,
  getLineWidth: 1,
  lineWidthUnits: 'pixels',
  pickable: true,
})

const labelsLayer = new MVTLayer({
  id: 'labels',
  data: TILES,
  loadOptions: {mvt: {layers: ['labels']}},
  pointType: 'circle',
  getPointRadius: getPointRadius,
  pointRadiusUnits: 'pixels',
  pointRadiusMinPixels: 1,
  filled: false,
  stroked: true,
  getLineColor: COLORS.label,
  getLineWidth: 1,
  lineWidthUnits: 'pixels',
  pickable: true,
})

const LAYERS = [gridLayer, tracksLayer, albumsLayer, artistsLayer, labelsLayer]


/** Fetches entity info from the API, validating the response status. */
function fetchEntityInfo(entityType: string, rowid: number | string, signal: AbortSignal) {
  return fetch(`/api/panel?rowid=${rowid}&entity_name=${entityType}`, {signal})
    .then(r => {
      if (!r.ok) throw new Error(r.statusText);
      return r.json()
    })
}

/** Root application component — owns view state, selection, and wires navigation to the map. */
let hashTimer: ReturnType<typeof setTimeout> | null = null
const MAP_VIEW = new MapView({repeat: false})
export default function App() {
  const {longitude, latitude, zoom, entity: initEntity, rowid: initRowid} = parseHash()

  const [viewState, setViewState] = useState<MapViewState>({...INITIAL_VIEW_STATE, longitude, latitude, zoom})
  const [selection, setSelection] = useState<Selection | null>(initEntity && initRowid ? {status: 'loading'} : null)
  const [history, setHistory] = useState<Selection[]>([])
  const nextSelection = useRef(makeAbortable())
  const selectionRef = useRef<Selection | null>(null)
  selectionRef.current = selection

  /** Fetches entity info and updates the panel selection, without moving the map. */
  const selectEntity = useCallback((entityType: string, rowid: number) => {
    if (selectionRef.current?.status === 'loaded')
      setHistory(prev => [...prev.slice(-(MAX_HISTORY - 1)), selectionRef.current!])

    updateHash({entity: entityType, rowid})
    setSelection({status: 'loading'})
    const signal = nextSelection.current.nextSignal()
    fetchEntityInfo(entityType, rowid, signal)
      .then(data => setSelection({status: 'loaded', entity_type: entityType, ...data}))
      .catch(err => {
        if (err.name !== 'AbortError') setSelection({status: 'error'})
      })
  }, [])

  /** Animates the map camera to the given coordinates. */
  function flyTo(lon: number, lat: number) {
    setViewState(prev => ({
      ...prev,
      longitude: lon,
      latitude: lat,
      zoom: 8,
      transitionDuration: 1000,
      transitionInterpolator: new FlyToInterpolator(),
    }))
  }

  /** Flies the map to the given coordinates and selects the entity. */
  const navigate = useCallback(
      (entityType: string, rowid: number, lon: number, lat: number) => {
      flyTo(lon, lat)
      selectEntity(entityType, rowid)
    }, [selectEntity]
  )

  /** Pops the last history entry and restores it without re-fetching, then fly to its lcoation. */
  const goBack = useCallback(() => {
    setHistory(prev => {
      const entry = prev[prev.length - 1]
      if (!entry || entry.status !== 'loaded') return prev
      setSelection(entry)
      updateHash({entity: entry.entity_type, rowid: getRowid(entry)})
      flyTo(entry.lon, entry.lat)
      return prev.slice(0, -1)
    })
  }, [])

  /** Debounces URL hash updates from the current map view state. */
  function debounceHashUpdate(vs: MapViewState) {
    // Debounce hash updates: fly animations fire onViewStateChange every frame,
    // and iOS Safari throws SecurityError if replaceState exceeds 100 calls/10s.
    if (hashTimer) clearTimeout(hashTimer)
    hashTimer = setTimeout(() => {
      updateHash({
        lon: vs.longitude.toFixed(4),
        lat: vs.latitude.toFixed(4),
        z: vs.zoom.toFixed(2),
      })
      hashTimer = null
    }, 100)
  }

  /** Closes the panel and clears selection-related local state. */
  function handlePanelClose() {
    nextSelection.current.cancel()
    setSelection(null)
    setHistory([])
    updateHash({entity: null, rowid: null})
  }

  /** Fetches entity info from URL hash on initial mount, without moving the map. */
  useEffect(() => {
    const {entity, rowid} = parseHash()
    if (!entity || !rowid) return
    const signal = nextSelection.current.nextSignal()
    fetchEntityInfo(entity, rowid, signal)
      .then(data => setSelection({status: 'loaded', entity_type: entity, ...data}))
      .catch(err => {
        if (err.name !== 'AbortError') setSelection({status: 'error'})
      })
  }, [])

  /** Handles clicks on pickable map dots — selects the entity without moving the map. */
  function handleMapClick(info: PickingInfo) {
    if (!info.object || !info.layer) return
    const p = (info.object as { properties: Record<string, number> }).properties
    switch (info.layer.id) {
      case 'tracks':
        return selectEntity('track', p.track_rowid)
      case 'albums':
        return selectEntity('album', p.album_rowid)
      case 'artists':
        return selectEntity('artist', p.artist_rowid)
      case 'labels':
        return selectEntity('label', p.label_rowid)
    }
  }

  return (
    <div className="relative w-screen h-screen">
      <DeckGL
        viewState={viewState}
        controller={true}
        onViewStateChange={({viewState: vs}) => {
          setViewState(vs)
          debounceHashUpdate(vs)
        }}
        layers={LAYERS}
        views={MAP_VIEW}
        onClick={handleMapClick}
        getCursor={({isHovering}) => isHovering ? 'pointer' : 'default'}
      />
      <Search navigate={navigate}/>
      <Panel
        selection={selection}
        navigate={navigate}
        onClose={handlePanelClose}
        goBack={history.length > 0 ? goBack : null}
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
