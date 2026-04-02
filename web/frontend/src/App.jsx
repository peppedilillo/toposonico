import {useEffect, useRef, useState} from 'react'
import maplibregl from 'maplibre-gl'
import 'maplibre-gl/dist/maplibre-gl.css'

import {LAYERS} from './layers.js'
import colors from './theme.js'

const STYLE = {
  version: 8,
  sources: {},
  layers: [
    {
      id: 'background',
      type: 'background',
      paint: {'background-color': colors.background},
    },
  ],
}

const START_ZOOM = 4

const gridLines = {type: 'FeatureCollection', features: []}
for (let lon = -180; lon <= 180; lon += 5) {
  gridLines.features.push({
    type: 'Feature',
    geometry: {
      type: 'LineString',
      coordinates: [
        [lon, -90],
        [lon, 90],
      ],
    },
  })
}
for (let lat = -90; lat <= 90; lat += 5) {
  gridLines.features.push({
    type: 'Feature',
    geometry: {
      type: 'LineString',
      coordinates: [
        [-180, lat],
        [180, lat],
      ],
    },
  })
}

function parseHash() {
  const params = new URLSearchParams(window.location.hash.slice(1))
  return {
    z: Number.parseFloat(params.get('z')) || START_ZOOM,
    lon: Number.parseFloat(params.get('lon')) || 0,
    lat: Number.parseFloat(params.get('lat')) || 0,
  }
}

function updateHash(map) {
  const center = map.getCenter()
  const params = new URLSearchParams(window.location.hash.slice(1))
  params.set('z', map.getZoom().toFixed(2))
  params.set('lon', center.lng.toFixed(4))
  params.set('lat', center.lat.toFixed(4))
  history.replaceState(null, '', `#${params.toString()}`)
}

function App() {
  const containerRef = useRef(null)
  const mapRef = useRef(null)
  const [zoom, setZoom] = useState(START_ZOOM)
  const [cursor, setCursor] = useState({x: 0, y: 0})

  useEffect(() => {
    const {z, lon, lat} = parseHash()
    const map = new maplibregl.Map({
      container: containerRef.current,
      style: STYLE,
      center: [lon, lat],
      zoom: z,
      minZoom: 5,
      maxBounds: [
        [-25, -25],
        [25, 25],
      ],
      attributionControl: false,
      scrollZoom: {around: 'center'},
    })
    mapRef.current = map

    map.on('load', () => {
      map.addSource('grid', {type: 'geojson', data: gridLines})
      map.addSource('tiles', {
        type: 'vector',
        tiles: [`${window.location.origin}/tiles/{z}/{x}/{y}.pbf`],
        maxzoom: 12,
      })

      map.addLayer({
        id: 'grid',
        type: 'line',
        source: 'grid',
        paint: {'line-color': colors.border, 'line-width': 1},
      })

      LAYERS.forEach(({id, sourceLayer, radius, color, opacity}) => {
        map.addLayer({
          id,
          type: 'circle',
          source: 'tiles',
          'source-layer': sourceLayer,
          paint: {
            'circle-radius': ['coalesce', ['get', 'logcount'], radius],
            'circle-color': color,
            'circle-opacity': opacity,
          },
        })
        map.on('mousemove', id, () => {
          map.getCanvas().style.cursor = 'pointer'
        })
        map.on('mouseleave', id, () => {
          map.getCanvas().style.cursor = ''
        })
      })
    })

    map.on('moveend', () => updateHash(map))
    map.on('zoom', () => setZoom(map.getZoom()))
    map.on('mousemove', (event) => {
      setCursor({x: event.lngLat.lng, y: event.lngLat.lat})
    })

    return () => map.remove()
  }, [])

  return (
    <div className="relative h-screen w-screen overflow-hidden bg-black text-white">
      <div ref={containerRef} className="h-full w-full"/>
      <div
        className="pointer-events-none absolute top-3 right-3 rounded-md bg-black/70 px-3 py-2 font-mono text-xs text-zinc-300">
        z {zoom.toFixed(2)} x {cursor.x.toFixed(4)} y {cursor.y.toFixed(4)}
      </div>
    </div>
  )
}

export default App
