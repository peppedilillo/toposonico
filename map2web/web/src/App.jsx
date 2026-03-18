import { useCallback, useEffect, useRef, useState } from "react";
import maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";

import colors from "./theme.js";
import Search from "./Search.jsx";
import Panel from "./Panel.jsx";
import { LAYERS } from "./layers.js";

const STYLE = {
    version: 8,
    glyphs: "/fonts/IBMPlexMono/{fontstack}/{range}.pbf",
    sources: {},
    layers: [
        {
            id: "background",
            type: "background",
            paint: { "background-color": colors.background },
        },
    ],
};

const gridLines = { type: "FeatureCollection", features: [] };
for (let lon = -180; lon <= 180; lon += 5) {
    gridLines.features.push({
        type: "Feature",
        geometry: {
            type: "LineString",
            coordinates: [
                [lon, -90],
                [lon, 90],
            ],
        },
    });
}
for (let lat = -90; lat <= 90; lat += 5) {
    gridLines.features.push({
        type: "Feature",
        geometry: {
            type: "LineString",
            coordinates: [
                [-180, lat],
                [180, lat],
            ],
        },
    });
}

const STARTZOOM = 4;

export default function App() {
    const containerRef = useRef(null);
    const mapRef = useRef(null);
    const [selection, setSelection] = useState(null);
    const [results, setResults] = useState([]);
    const [zoom, setZoom] = useState(STARTZOOM);
    const [cursor, setCursor] = useState({ x: 0, y: 0 });

    const selectEntity = useCallback((entityType, rowid) => {
        setSelection({ loading: true, entityType });
        fetch(`/api/info?q=${rowid}&entity=${entityType}`)
            .then((r) => r.json())
            .then((data) => setSelection({ entityType, ...data }))
            .catch(() => setSelection({ entityType, error: true }));
    }, []);

    const navigate = useCallback(
        (entityType, rowid, lon, lat) => {
            selectEntity(entityType, rowid);
            mapRef.current.flyTo({
                center: [lon, lat],
                zoom: 9,
                essential: true,
            });
        },
        [selectEntity],
    );

    useEffect(() => {
        const map = new maplibregl.Map({
            container: containerRef.current,
            style: STYLE,
            center: [0, 0],
            zoom: STARTZOOM,
            minZoom: 3,
            maxBounds: [
                [-60, -60],
                [60, 60],
            ],
            attributionControl: false,
        });
        mapRef.current = map;

        map.on("load", () => {
            map.addSource("grid", { type: "geojson", data: gridLines });
            map.addSource("tiles", {
                type: "vector",
                tiles: [`${window.location.origin}/tiles/{z}/{x}/{y}.pbf`],
                maxzoom: 11,
            });
            map.addLayer({
                id: "grid",
                type: "line",
                source: "grid",
                paint: { "line-color": colors.border, "line-width": 1 },
            });
            LAYERS.forEach(
                ({
                    id,
                    sourceLayer,
                    char,
                    size,
                    color,
                    opacity,
                    entityType,
                    rowidProp,
                }) => {
                    map.addLayer({
                        id,
                        type: "symbol",
                        source: "tiles",
                        "source-layer": sourceLayer,
                        layout: {
                            "text-field": char,
                            "text-size": size,
                            "text-font": ["IBM Plex Mono Regular"],
                            "text-allow-overlap": true,
                        },
                        paint: {
                            "text-color": color,
                            "text-opacity": opacity,
                        },
                    });
                    map.on("mousemove", id, () => {
                        map.getCanvas().style.cursor = "pointer";
                    });
                    map.on("mouseleave", id, () => {
                        map.getCanvas().style.cursor = "";
                    });
                    map.on("click", id, (e) => {
                        const p = e.features[0].properties;
                        selectEntity(entityType, p[rowidProp]);
                    });
                },
            );
            // The only goal of the next lines is to give the user a way to hide
            // searchbar dropdown and panel when he clicks on an empty space on the map.
            map.on("click", (e) => {
                const hit = map.queryRenderedFeatures(e.point, { layers: LAYERS.map(l => l.id) });
                if (!hit.length) {
                    setSelection(null);
                }
            });
        });

        map.on("zoom", () => setZoom(map.getZoom()));
        map.on("mousemove", (e) =>
            setCursor({ x: e.lngLat.lng, y: e.lngLat.lat }),
        );
        return () => map.remove();
    }, []);

    return (
        <div className="relative w-screen h-screen text-white font-normal">
            <div ref={containerRef} className="w-full h-full" />
            <Search
                navigate={navigate}
                results={results}
                setResults={setResults}
            />
            <Panel
                selection={selection}
                navigate={navigate}
                onClose={() => setSelection(null)}
            />
            <div className="absolute top-3 right-3 text-muted text-xs font-sans pointer-events-none">
                z {zoom.toFixed(2)} x {cursor.x.toFixed(4)} y{" "}
                {cursor.y.toFixed(4)}
            </div>
        </div>
    );
}
