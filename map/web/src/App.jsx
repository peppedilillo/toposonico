import {useEffect, useRef} from 'react';
import maplibregl from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';

const TILE_URL = `${window.location.origin}/tiles/{z}/{x}/{y}.pbf`;

const STYLE = {
    version: 8,
    sources: {},
    layers: [{id: 'background', type: 'background', paint: {'background-color': '#0d0d12'}}],
};

export default function App() {
    const containerRef = useRef(null);

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

            map.on('mouseenter', 'tracks', () => {
                map.getCanvas().style.cursor = 'pointer';
            });
            map.on('mouseleave', 'tracks', () => {
                map.getCanvas().style.cursor = '';
            });
        });

        return () => map.remove();
    }, []);

    return <div ref={containerRef} style={{width: '100vw', height: '100vh'}}/>;
}