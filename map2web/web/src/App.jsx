import {useEffect, useRef, useState} from 'react';
import maplibregl from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';

const TILE_URL = `${window.location.origin}/tiles/{z}/{x}/{y}.pbf`;

const STYLE = {
    version: 8,
    sources: {},
    glyphs: '/fonts/IBMPlexMono/{fontstack}/{range}.pbf',
    layers: [{id: 'background', type: 'background', paint: {'background-color': '#0d0d12'}}],
};

export default function App() {
    const containerRef = useRef(null);
    const [tooltip, setTooltip] = useState(null); // {x, y, artist_name, track_name}

    useEffect(() => {
        const map = new maplibregl.Map({
            container: containerRef.current,
            style: STYLE,
            center: [0, 0],
            minZoom: 3,
        });

        map.on('load', () => {
            map.addSource('tracks', {
                type: 'vector',
                tiles: [TILE_URL],
                maxzoom: 11,
            });

            map.addLayer({
                id: 'tracks',
                type: 'circle',
                source: 'tracks',
                'source-layer': 'tracks',
                paint: {
                    'circle-radius': [
                        'interpolate', ['linear'], ['zoom'],
                        0, ['interpolate', ['linear'], ['get', 'track_popularity'], 0, 1,   100, 2.5],
                        5, ['interpolate', ['linear'], ['get', 'track_popularity'], 0, 1.5, 100, 4],
                        7, ['interpolate', ['linear'], ['get', 'track_popularity'], 0, 2,   100, 6],
                        14,['interpolate', ['linear'], ['get', 'track_popularity'], 0, 3,   100, 9],
                    ],
                    'circle-opacity': [
                        'interpolate', ['linear'], ['zoom'],
                        2, 0.4,
                        5, 0.6,
                        8, 0.85,
                    ],
                    'circle-color': [
                        'interpolate', ['linear'], ['get', 'track_popularity'],
                        0,   '#0033ff',
                        20,  '#6600ff',
                        40,  '#cc00ff',
                        60,  '#ff00aa',
                        80,  '#ff3300',
                        100, '#ffee00',
                    ],
                },
            });

            map.addLayer({
                id: 'tracks-labels',
                type: 'symbol',
                source: 'tracks',
                'source-layer': 'tracks',
                minzoom: 4,
                filter: ['>', ['get', 'track_popularity'],
                    ['step', ['zoom'],
                        70,
                        6, 60,
                        7, 55,
                        8, 50,
                        9, 45,
                    ]
                ],
                layout: {
                    'text-field': ['format',
                        ['get', 'artist_name'], {'font-scale': 0.85},
                        '\n', {},
                        ['get', 'track_name'], {'font-scale': 1},
                    ],
                    'text-font': ['IBM Plex Mono Regular'],
                    'text-size': 11,
                    'text-offset': [0, 1.2],
                    'text-anchor': 'top',
                    'text-max-width': 12,
                },
                paint: {
                    'text-color': '#ffffff',
                    'text-halo-color': 'rgba(6, 8, 16, 0.95)',
                    'text-halo-width': 1.2,
                },
            });

            map.on('mousemove', 'tracks', (e) => {
                map.getCanvas().style.cursor = 'pointer';
                const props = e.features[0].properties;
                setTooltip({
                    x: e.point.x,
                    y: e.point.y,
                    artist_name: props.artist_name,
                    track_name: props.track_name,
                });
            });

            map.on('mouseleave', 'tracks', () => {
                map.getCanvas().style.cursor = '';
                setTooltip(null);
            });
        });

        return () => map.remove();
    }, []);

    return (
        <div style={{position: 'relative', width: '100vw', height: '100vh'}}>
            <div ref={containerRef} style={{width: '100%', height: '100%'}}/>
            {tooltip && (
                <div style={{
                    position: 'absolute',
                    left: tooltip.x + 12,
                    top: tooltip.y + 12,
                    pointerEvents: 'none',
                    background: 'rgba(13, 13, 18, 0.85)',
                    color: 'rgba(220, 230, 255, 0.9)',
                    fontFamily: 'monospace',
                    fontSize: '12px',
                    lineHeight: '1.5',
                    padding: '6px 10px',
                    whiteSpace: 'nowrap',
                }}>
                    <div style={{opacity: 0.6}}>{tooltip.artist_name}</div>
                    <div>{tooltip.track_name}</div>
                </div>
            )}
        </div>
    );
}
