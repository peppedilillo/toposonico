import {useCallback, useState} from 'react'
import Search from './Search'
import {FlyToInterpolator, MapView} from '@deck.gl/core'
import {MVTLayer} from '@deck.gl/geo-layers'
import {GeoJsonLayer} from '@deck.gl/layers'
import DeckGL from '@deck.gl/react'
import type {Feature, FeatureCollection, Geometry} from 'geojson'


const INITIAL_VIEW_STATE = {longitude: 4.28, latitude: -7.21, zoom: 5, pitch: 10, bearing: 0}
const TILES = '/tiles/{z}/{x}/{y}.pbf'

/** Reads a CSS custom property and returns a deck.gl-compatible [r, g, b, a] color array. */
export function cssColor(variable: string, alpha = 255): [number, number, number, number] {
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
function gridLayer() {
  return new GeoJsonLayer({
    id: 'grid',
    data: gridLines,
    stroked: true,
    filled: false,
    getLineColor: COLORS.border,
    getLineWidth: 1,
    lineWidthUnits: 'pixels',
  })
}

type TileProperties = {
  logcount?: number
}

type TileFeature = Feature<Geometry, TileProperties>

/** Maps a tile feature's logcount to a pixel radius, falling back to 1. */
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

/** MVT layer factory functions — one per entity type, each scoped to its source layer. */
function tracksLayer() {
  return new MVTLayer<TileProperties>({
    ...TILE_BASE,
    id: 'tracks',
    loadOptions: {mvt: {layers: ['tracks']}},
    getFillColor: COLORS.track,
  })
}

function albumsLayer() {
  return new MVTLayer<TileProperties>({
    ...TILE_BASE,
    id: 'albums',
    loadOptions: {mvt: {layers: ['albums']}},
    getFillColor: COLORS.album,
  })
}

function artistsLayer() {
  return new MVTLayer<TileProperties>({
    ...TILE_BASE,
    id: 'artists',
    loadOptions: {mvt: {layers: ['artists']}},
    getFillColor: COLORS.artist,
  })
}

function labelsLayer() {
  return new MVTLayer<TileProperties>({
    ...TILE_BASE,
    id: 'labels',
    loadOptions: {mvt: {layers: ['labels']}},
    getFillColor: COLORS.label,
  })
}


/** Root application component — owns view state and wires navigation to the map. */
export default function App() {
  const [viewState, setViewState] = useState<object>(INITIAL_VIEW_STATE)

  /** Flies the map to the given coordinates, preserving current pitch and bearing. */
  const navigate = useCallback((_entityType: string, _rowid: number, lon: number, lat: number) => {
    setViewState(prev => ({
      ...prev,
      longitude: lon,
      latitude: lat,
      zoom: 10,
      transitionDuration: 1000,
      transitionInterpolator: new FlyToInterpolator(),
    }))
  }, [])

  return (
    <div className="relative w-screen h-screen">
      <DeckGL
        viewState={viewState}
        controller={{minZoom: 5, maxZoom: 14}}
        onViewStateChange={({viewState: vs}) => setViewState(vs)}
        layers={[gridLayer(), tracksLayer(), albumsLayer(), artistsLayer(), labelsLayer()]}
        views={new MapView({repeat: false})}
      />
      <Search navigate={navigate}/>
    </div>
  )
}
