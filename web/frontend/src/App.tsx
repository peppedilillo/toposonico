import {useCallback, useState} from 'react'
import Search from './Search'
import {FlyToInterpolator, MapView} from '@deck.gl/core'
import {MVTLayer} from '@deck.gl/geo-layers'
import {GeoJsonLayer} from '@deck.gl/layers'
import DeckGL from '@deck.gl/react'
import type {Feature, FeatureCollection, Geometry} from 'geojson'


const INITIAL_VIEW_STATE = {longitude: 4.28, latitude: -7.21, zoom: 5, pitch: 10, bearing: 0}
const TILES = '/tiles/{z}/{x}/{y}.pbf'
const MIN_ZOOM = 5
const MAX_ZOOM = 14
const TILE_BOUNDS = {
  minLongitude: -22.5,
  minLatitude: -22.5,
  maxLongitude: 22.5,
  maxLatitude: 22.5,
}

// Reads a CSS custom property and returns a deck.gl-compatible [r, g, b, a] color array.
export function cssColor(variable: string, alpha = 255): [number, number, number, number] {
  const hex = getComputedStyle(document.documentElement).getPropertyValue(variable).trim()
  const n = parseInt(hex.slice(1), 16)
  return [(n >> 16) & 255, (n >> 8) & 255, n & 255, alpha]
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

// Renders the lat/lon graticule as a GeoJSON line layer.
function gridLayer() {
  return new GeoJsonLayer({
    id: 'grid',
    data: gridLines,
    stroked: true,
    filled: false,
    getLineColor: cssColor('--color-border'),
    getLineWidth: 1,
    lineWidthUnits: 'pixels',
  })
}

type TileProperties = {
  logcount?: number
}

type TileFeature = Feature<Geometry, TileProperties>

// Maps a tile feature's logcount to a pixel radius, falling back to 1.
function getRadius(f: TileFeature) {
  return f.properties.logcount ?? 1
}

const TILE_BASE = {
  data: TILES,
  pointType: 'circle' as const,
  getPointRadius: getRadius,
  pointRadiusUnits: 'pixels' as const,
  pointRadiusMinPixels: 1,
  pointRadiusMaxPixels: 15,
  pickable: true,
}

// MVT layer factory functions — one per entity type, each scoped to its source layer.
function tracksLayer() {
  const color = cssColor('--color-track', 180)

  return new MVTLayer<TileProperties>({
    ...TILE_BASE,
    id: 'tracks',
    loadOptions: {mvt: {layers: ['tracks']}},
    getFillColor: color,
  })
}

function albumsLayer() {
  const color = cssColor('--color-album', 180)

  return new MVTLayer<TileProperties>({
    ...TILE_BASE,
    id: 'albums',
    loadOptions: {mvt: {layers: ['albums']}},
    getFillColor: color,
  })
}

function artistsLayer() {
  const color = cssColor('--color-artist', 180)

  return new MVTLayer<TileProperties>({
    ...TILE_BASE,
    id: 'artists',
    loadOptions: {mvt: {layers: ['artists']}},
    getFillColor: color,
  })
}

function labelsLayer() {
  const color = cssColor('--color-label', 180)

  return new MVTLayer<TileProperties>({
    ...TILE_BASE,
    id: 'labels',
    loadOptions: {mvt: {layers: ['labels']}},
    getFillColor: color,
  })
}

function clamp(value: number, min: number, max: number) {
  return Math.min(Math.max(value, min), max)
}

// Clamps zoom and position to the tile bounds so the user can't pan off the data extent.
function clampViewState(viewState: typeof INITIAL_VIEW_STATE) {
  return {
    ...viewState,
    zoom: clamp(viewState.zoom, MIN_ZOOM, MAX_ZOOM),
    longitude: clamp(viewState.longitude, TILE_BOUNDS.minLongitude, TILE_BOUNDS.maxLongitude),
    latitude: clamp(viewState.latitude, TILE_BOUNDS.minLatitude, TILE_BOUNDS.maxLatitude),
  }
}

/** Root application component — owns view state and wires navigation to the map. */
export default function App() {
  const [viewState, setViewState] = useState<object>(INITIAL_VIEW_STATE)

  /** Flies the map to the given coordinates at a zoom appropriate for the entity type. */
  const navigate = useCallback((_entityType: string, _rowid: number, lon: number, lat: number) => {
    setViewState({
      ...INITIAL_VIEW_STATE,
      longitude: lon,
      latitude: lat,
      zoom: 10,
      transitionDuration: 1000,
      transitionInterpolator: new FlyToInterpolator(),
    })
  }, [])

  return (
    <div className="relative w-screen h-screen">
      <DeckGL
        viewState={viewState}
        controller={true}
        onViewStateChange={({viewState: nextViewState}) => {
          setViewState(clampViewState(nextViewState as typeof INITIAL_VIEW_STATE))
        }}
        layers={[gridLayer(), tracksLayer(), albumsLayer(), artistsLayer(), labelsLayer()]}
        views={new MapView({repeat: false})}
      />
      <Search navigate={navigate}/>
    </div>
  )
}
