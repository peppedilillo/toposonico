import {useCallback, useEffect, useState} from 'react'
import Search from './Search'
import Panel from './Panel'
import type {Selection} from './Panel'
import {FlyToInterpolator, MapView} from '@deck.gl/core'
import type {PickingInfo} from '@deck.gl/core'
import {MVTLayer} from '@deck.gl/geo-layers'
import {GeoJsonLayer} from '@deck.gl/layers'
import DeckGL from '@deck.gl/react'
import type {FeatureCollection} from 'geojson'


const INITIAL_VIEW_STATE = {longitude: 4.28, latitude: -7.21, zoom: 5, pitch: 10, bearing: 0}

/** Reads map state from the URL hash. Missing keys fall back to INITIAL_VIEW_STATE defaults. */
function parseHash() {
  const p = new URLSearchParams(window.location.hash.slice(1))
  const lon  = parseFloat(p.get('lon') ?? '')
  const lat  = parseFloat(p.get('lat') ?? '')
  const z    = parseFloat(p.get('z')   ?? '')
  return {
    longitude: isNaN(lon) ? INITIAL_VIEW_STATE.longitude : lon,
    latitude:  isNaN(lat) ? INITIAL_VIEW_STATE.latitude  : lat,
    zoom:      isNaN(z)   ? INITIAL_VIEW_STATE.zoom       : z,
    entity:    p.get('entity'),
    rowid:     p.get('rowid'),
  }
}

/**
 * Merges updates into the current URL hash via replaceState (no history entry).
 * Pass null for a value to remove that key.
 */
function updateHash(updates: Record<string, string | number | null>) {
  const p = new URLSearchParams(window.location.hash.slice(1))
  for (const [k, v] of Object.entries(updates))
    if (v == null) p.delete(k)
    else p.set(k, String(v))
  history.replaceState(null, '', '#' + p.toString())
}
const TILES = '/tiles/{z}/{x}/{y}.pbf'

/** Reads a CSS custom property and returns a deck.gl-compatible [r, g, b, a] color array. */
function cssColor(variable: string, alpha = 255): [number, number, number, number] {
  const hex = getComputedStyle(document.documentElement).getPropertyValue(variable).trim()
  const n = parseInt(hex.slice(1), 16)
  return [(n >> 16) & 255, (n >> 8) & 255, n & 255, alpha]
}


/** Colors computed once at module load — cssColor reads from the DOM so this must run after CSS is applied. */
const COLORS = {
  border: cssColor('--color-border'),
  track: cssColor('--color-track', 180),
  album: cssColor('--color-album', 180),
  artist: cssColor('--color-artist', 180),
  label: cssColor('--color-label', 180),
}

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


/** Renders the lat/lon graticule as a GeoJSON line layer. */
const gridLayer = new GeoJsonLayer({
  id: 'grid',
  data: gridLines,
  stroked: true,
  filled: false,
  getLineColor: COLORS.border,
  getLineWidth: 1,
  lineWidthUnits: 'pixels',
})


/** Maps a tile feature's logcount to a pixel radius. */
function getPointRadius(feature: {properties: {logcount: number}}) {
  return feature.properties.logcount
}

/** MVT layers — one per entity type, initialized once at module level. */
const tracksLayer = new MVTLayer({
  id: 'tracks',
  data: TILES,
  loadOptions: {mvt: {layers: ['tracks']}},
  pointType: 'circle',
  getPointRadius: getPointRadius,
  pointRadiusUnits: 'pixels',
  pointRadiusMinPixels: 1,
  pointRadiusMaxPixels: 15,
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
  pointRadiusMaxPixels: 15,
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
  pointRadiusMaxPixels: 15,
  getFillColor: COLORS.artist,
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
  pointRadiusMaxPixels: 15,
  getFillColor: COLORS.label,
  pickable: true,
})

const LAYERS = [gridLayer, tracksLayer, albumsLayer, artistsLayer, labelsLayer]
const MAP_VIEW = new MapView({repeat: false})


/** Root application component — owns view state, selection, and wires navigation to the map. */
export default function App() {
  const {longitude, latitude, zoom, entity: initEntity, rowid: initRowid} = parseHash()
  const [viewState, setViewState] = useState<object>({...INITIAL_VIEW_STATE, longitude, latitude, zoom})
  const [selection, setSelection] = useState<Selection | null>(
    initEntity && initRowid ? {status: 'loading'} : null
  )

  /** Fetches entity info and updates the panel selection, without moving the map. */
  const selectEntity = useCallback((entityType: string, rowid: number) => {
    updateHash({entity: entityType, rowid})
    setSelection({status: 'loading'})
    fetch(`/api/info?rowid=${rowid}&entity_name=${entityType}`)
      .then(r => r.json())
      .then(data => setSelection({status: 'loaded', entity_type: entityType, ...data}))
      .catch(() => setSelection({status: 'error'}))
  }, [])

  /** Flies the map to the given coordinates and selects the entity. */
  const navigate = useCallback((entityType: string, rowid: number, lon: number, lat: number) => {
    setViewState(prev => ({
      ...prev,
      longitude: lon,
      latitude: lat,
      zoom: 10,
      transitionDuration: 1000,
      transitionInterpolator: new FlyToInterpolator(),
    }))
    selectEntity(entityType, rowid)
  }, [selectEntity])

  /** Fetches entity info from URL hash on initial mount, without moving the map. */
  useEffect(() => {
    const {entity, rowid} = parseHash()
    if (!entity || !rowid) return
    fetch(`/api/info?rowid=${rowid}&entity_name=${entity}`)
      .then(r => r.json())
      .then(data => setSelection({status: 'loaded', entity_type: entity, ...data}))
      .catch(() => setSelection({status: 'error'}))
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  /** Handles clicks on pickable map dots — selects the entity without moving the map. */
  function handleMapClick(info: PickingInfo) {
    if (!info.object || !info.layer) return
    const p = (info.object as {properties: Record<string, number>}).properties
    switch (info.layer.id) {
      case 'tracks':  return selectEntity('track',  p.track_rowid)
      case 'albums':  return selectEntity('album',  p.album_rowid)
      case 'artists': return selectEntity('artist', p.artist_rowid)
      case 'labels':  return selectEntity('label',  p.label_rowid)
    }
  }

  return (
    <div className="relative w-screen h-screen">
      <DeckGL
        viewState={viewState}
        controller={{minZoom: 5, maxZoom: 14}}
        onViewStateChange={({viewState: vs}) => {
          setViewState(vs)
          const {longitude, latitude, zoom} = vs as {longitude: number; latitude: number; zoom: number}
          updateHash({lon: longitude.toFixed(4), lat: latitude.toFixed(4), z: zoom.toFixed(2)})
        }}
        layers={LAYERS}
        views={MAP_VIEW}
        onClick={handleMapClick}
        getCursor={({isHovering}) => isHovering ? 'pointer' : 'default'}
      />
      <Search navigate={navigate}/>
      <Panel selection={selection} navigate={navigate} onClose={() => {
        setSelection(null)
        updateHash({entity: null, rowid: null})
      }}/>
    </div>
  )
}
