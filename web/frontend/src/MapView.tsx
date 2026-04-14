import {useEffect, useRef} from 'react'
import maplibregl from 'maplibre-gl'
import 'maplibre-gl/dist/maplibre-gl.css'
import type {FeatureCollection} from 'geojson'

import type {EntityType} from "./utils.ts";

export type ViewState = {
  lon: number
  lat: number
  zoom: number
}

export type MapCommand =
  | null
  | {
      type: 'flyTo'
      center: [number, number]
      zoom?: number
    }

type MapViewProps = {
  initialView: ViewState
  command: MapCommand
  onMoveEnd: (view: ViewState) => void
  onFeatureSelect: (entityType: EntityType, rowid: number) => void
}

const INITIAL_VIEW = {
  minZoom: 5,
  maxZoom: 14,
  pitch: 10,
}

// MapLibre requires absolute URLs for tile sources. In dev the env var is a relative path
// and we prepend the page origin so it works on both localhost and LAN.
const TILE_URL_RAW = (import.meta.env.VITE_TILES_URL as string | undefined) ?? '/tiles/{z}/{x}/{y}.pbf'
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

/** Parses a feature property into a numeric rowid when present. */
function getNumericRowid(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) return value
  if (typeof value === 'string' && value.trim() !== '') {
    const parsed = Number(value)
    if (Number.isFinite(parsed)) return parsed
  }
  return null
}

/** MapLibre wrapper responsible only for map rendering and imperative camera commands. */
export default function MapView({initialView, command, onMoveEnd, onFeatureSelect}: MapViewProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const mapRef = useRef<maplibregl.Map | null>(null)

  useEffect(() => {
    const map = new maplibregl.Map({
      container: containerRef.current!,
      style: {
        version: 8,
        sources: {},
        layers: [{id: 'background', type: 'background', paint: {'background-color': '#0d0d12'}}],
      },
      center: [initialView.lon, initialView.lat],
      zoom: initialView.zoom,
      minZoom: INITIAL_VIEW.minZoom,
      maxZoom: INITIAL_VIEW.maxZoom,
      pitch: INITIAL_VIEW.pitch,
      renderWorldCopies: false,
      attributionControl: false,
    })
    mapRef.current = map

    map.on('load', () => {
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

      const bindClick = (layerId: string, entityType: EntityType, propertyName: string) => {
        map.on('click', layerId, (e) => {
          const rowid = getNumericRowid(e.features?.[0]?.properties?.[propertyName])
          if (rowid != null) onFeatureSelect(entityType, rowid)
        })
      }

      bindClick('tracks', 'track', 'track_rowid')
      bindClick('albums', 'album', 'album_rowid')
      bindClick('artists', 'artist', 'artist_rowid')
      bindClick('labels', 'label', 'label_rowid')

      for (const id of ['tracks', 'albums', 'artists', 'labels']) {
        map.on('mouseenter', id, () => {
          map.getCanvas().style.cursor = 'pointer'
        })
        map.on('mouseleave', id, () => {
          map.getCanvas().style.cursor = ''
        })
      }
    })

    map.on('moveend', () => {
      const center = map.getCenter()
      onMoveEnd({
        lon: center.lng,
        lat: center.lat,
        zoom: map.getZoom(),
      })
    })

    return () => map.remove()
  }, [initialView.lat, initialView.lon, initialView.zoom, onFeatureSelect, onMoveEnd])

  useEffect(() => {
    if (!command || command.type !== 'flyTo') return
    mapRef.current?.flyTo({center: command.center, zoom: command.zoom})
  }, [command])

  return <div ref={containerRef} className="w-full h-full select-none"/>
}
