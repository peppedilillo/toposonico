import { useState } from 'react'
import { MapView } from '@deck.gl/core'
import { MVTLayer } from '@deck.gl/geo-layers'
import { GeoJsonLayer } from '@deck.gl/layers'
import DeckGL from '@deck.gl/react'
import type { Feature, FeatureCollection, Geometry } from 'geojson'
import colors, { rgba } from './theme'

const INITIAL_VIEW_STATE = { longitude: 4.28, latitude: -7.21, zoom: 5, pitch: 10, bearing: 0 }
const TILES = import.meta.env.VITE_TILE_URL ?? 'http://localhost:8081/tiles/{z}/{x}/{y}.pbf'
const MIN_ZOOM = 5
const MAX_ZOOM = 14
const TILE_BOUNDS = {
  minLongitude: -22.5,
  minLatitude: -22.5,
  maxLongitude: 22.5,
  maxLatitude: 22.5,
}

const gridLines: FeatureCollection = { type: 'FeatureCollection', features: [] }
for (let lon = -180; lon <= 180; lon += 5) {
  gridLines.features.push({
    type: 'Feature',
    geometry: { type: 'LineString', coordinates: [[lon, -90], [lon, 90]] },
    properties: {},
  })
}
for (let lat = -90; lat <= 90; lat += 5) {
  gridLines.features.push({
    type: 'Feature',
    geometry: { type: 'LineString', coordinates: [[-180, lat], [180, lat]] },
    properties: {},
  })
}

function gridLayer() {
  return new GeoJsonLayer({
    id: 'grid',
    data: gridLines,
    stroked: true,
    filled: false,
    getLineColor: rgba(colors.border),
    getLineWidth: 1,
    lineWidthUnits: 'pixels',
  })
}

type TileProperties = {
  logcount?: number
}

type TileFeature = Feature<Geometry, TileProperties>

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

function tracksLayer() {
  const color = rgba(colors.track, 180)

  return new MVTLayer<TileProperties>({
    ...TILE_BASE,
    id: 'tracks',
    loadOptions: { mvt: { layers: ['tracks'] } },
    getFillColor: color,
  })
}

function albumsLayer() {
  const color = rgba(colors.album, 180)

  return new MVTLayer<TileProperties>({
    ...TILE_BASE,
    id: 'albums',
    loadOptions: { mvt: { layers: ['albums'] } },
    getFillColor: color,
  })
}

function artistsLayer() {
  const color = rgba(colors.artist, 180)

  return new MVTLayer<TileProperties>({
    ...TILE_BASE,
    id: 'artists',
    loadOptions: { mvt: { layers: ['artists'] } },
    getFillColor: color,
  })
}

function labelsLayer() {
  const color = rgba(colors.label, 180)

  return new MVTLayer<TileProperties>({
    ...TILE_BASE,
    id: 'labels',
    loadOptions: { mvt: { layers: ['labels'] } },
    getFillColor: color,
  })
}

function clamp(value: number, min: number, max: number) {
  return Math.min(Math.max(value, min), max)
}

function clampViewState(viewState: typeof INITIAL_VIEW_STATE) {
  return {
    ...viewState,
    zoom: clamp(viewState.zoom, MIN_ZOOM, MAX_ZOOM),
    longitude: clamp(viewState.longitude, TILE_BOUNDS.minLongitude, TILE_BOUNDS.maxLongitude),
    latitude: clamp(viewState.latitude, TILE_BOUNDS.minLatitude, TILE_BOUNDS.maxLatitude),
  }
}

export default function App() {
  const [viewState, setViewState] = useState(INITIAL_VIEW_STATE)

  return (
    <DeckGL
      viewState={viewState}
      controller
      onViewStateChange={({ viewState: nextViewState }) => {
        setViewState(clampViewState(nextViewState as typeof INITIAL_VIEW_STATE))
      }}
      layers={[gridLayer(), tracksLayer(), albumsLayer(), artistsLayer(), labelsLayer()]}
      views={new MapView({ repeat: false })}
    />
  )
}
