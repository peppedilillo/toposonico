import {useEffect, useRef, useState} from 'react';
import maplibregl from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';

import Search from "./Search.jsx";

const LAYERS = [
    {
        id: 'tracks', sourceLayer: 'tracks', char: '·', size: 16, color: '#3bda28', opacity: 0.7,
        tooltip: (p) => ({entityType: 'track', line1: p.artist_name, line2: p.track_name})
    },
    {
        id: 'artists', sourceLayer: 'artists', char: '*', size: 12, color: '#00ffbe', opacity: 1.0,
        tooltip: (p) => ({entityType: 'artist', line2: p.artist_name})
    },
    {
        id: 'albums', sourceLayer: 'albums', char: 'o', size: 12, color: '#f8edb0', opacity: 0.7,
        tooltip: (p) => ({entityType: 'album', line1: p.artist_name, line2: p.album_name})
    },
    {
        id: 'labels', sourceLayer: 'labels', char: 'P', size: 24, color: '#ca53fa', opacity: 1.0,
        tooltip: (p) => ({entityType: 'label', line2: p.label})
    },
];

const STYLE = {
    version: 8,
    glyphs: '/fonts/IBMPlexMono/{fontstack}/{range}.pbf',
    sources: {},
    layers: [{id: 'background', type: 'background', paint: {'background-color': '#000000'}}],
}

const STYLE_DEBUG = {
    position: 'absolute', bottom: 12, left: 12,
    color: 'white', fontFamily: 'monospace', fontSize: 11,
    pointerEvents: 'none',
}

const STYLE_HOVER = {
    position: 'absolute',
    background: 'rgba(13, 13, 18, 0.85)',
    color: 'rgba(220, 230, 255, 0.9)',
    fontFamily: 'monospace',
    fontSize: '12px',
    lineHeight: '1.5',
    padding: '6px 10px',
    whiteSpace: 'nowrap',
}

const gridLines = {type: 'FeatureCollection', features: []};
for (let lon = -180; lon <= 180; lon += 5) {
    gridLines.features.push({
        type: 'Feature', geometry: {
            type: 'LineString', coordinates: [[lon, -90], [lon, 90]]
        }
    });
}
for (let lat = -90; lat <= 90; lat += 5) {
    gridLines.features.push({
        type: 'Feature', geometry: {
            type: 'LineString', coordinates: [[-180, lat], [180, lat]]
        }
    });
}

export default function App() {
    const containerRef = useRef(null);
    const mapRef = useRef(null);
    const [tooltip, setTooltip] = useState(null); // {x, y, entityType, line1, line2}
    const [zoom, setZoom] = useState(3);
    const [cursor, setCursor] = useState({x: 0, y: 0});

    useEffect(() => {

        const map = new maplibregl.Map({
            container: containerRef.current,
            style: STYLE,
            center: [0., 0.],
            zoom: 4,
            minZoom: 3,
        });
        mapRef.current = map;

        map.on('load', () => {
            map.addSource('grid', {type: 'geojson', data: gridLines});
            map.addSource('tiles', {
                type: 'vector',
                tiles: [`${window.location.origin}/tiles/{z}/{x}/{y}.pbf`],
                maxzoom: 11,
            });
            map.addLayer({
                id: 'grid',
                type: 'line',
                source: 'grid',
                paint: {'line-color': '#303030', 'line-width': 1},
            });
            LAYERS.forEach(({id, sourceLayer, char, size, color, opacity, tooltip}) => {
                map.addLayer({
                    id,
                    type: 'symbol',
                    source: 'tiles',
                    'source-layer': sourceLayer,
                    layout: {
                        'text-field': char,
                        'text-size': size,
                        'text-font': ['IBM Plex Mono Regular'],
                        'text-allow-overlap': true,
                    },
                    paint: {
                        'text-color': color,
                        'text-opacity': opacity,
                    },
                });
                map.on('mousemove', id, (e) => {
                    map.getCanvas().style.cursor = 'pointer';
                    setTooltip({x: e.point.x, y: e.point.y, ...tooltip(e.features[0].properties)});
                });
                map.on('mouseleave', id, () => {
                    map.getCanvas().style.cursor = '';
                    setTooltip(null);
                })
            })
        });

        map.on('zoom', () => setZoom(map.getZoom()));
        map.on('mousemove', (e) => setCursor({x: e.lngLat.lng, y: e.lngLat.lat}));
        return () => map.remove();
    }, []);

    return (
        <div style={{position: 'relative', width: '100vw', height: '100vh'}}>
            <div ref={containerRef} style={{width: '100%', height: '100%'}}/>
            <Search mapRef={mapRef} />
            <div style={STYLE_DEBUG}>
                z {zoom.toFixed(2)} x {cursor.x.toFixed(4)} y {cursor.y.toFixed(4)}
            </div>
            {tooltip && (
                <div style={{...STYLE_HOVER, left: tooltip.x + 12, top: tooltip.y + 12}}>
                    <div style={{opacity: 0.6}}>{tooltip.entityType}</div>
                    <div style={{opacity: 0.6}}>{tooltip.line1}</div>
                    <div>{tooltip.line2}</div>
                </div>
            )}
        </div>
    );
}