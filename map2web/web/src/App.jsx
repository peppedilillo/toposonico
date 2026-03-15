import {useEffect, useRef, useState} from "react";
import maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";

import colors from "./theme.js";
import Search from "./Search.jsx";
import {LAYERS} from "./layers.js";

const STYLE = {
    version: 8,
    glyphs: "/fonts/IBMPlexMono/{fontstack}/{range}.pbf",
    sources: {},
    layers: [
        {
            id: "background",
            type: "background",
            paint: {"background-color": colors.background},
        },
    ],
};

const gridLines = {type: "FeatureCollection", features: []};
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
            center: [0, 0],
            zoom: 4,
            minZoom: 3,
            maxBounds: [
                [-60, -60],
                [60, 60],
            ],
        });
        mapRef.current = map;

        map.on("load", () => {
            map.addSource("grid", {type: "geojson", data: gridLines});
            map.addSource("tiles", {
                type: "vector",
                tiles: [`${window.location.origin}/tiles/{z}/{x}/{y}.pbf`],
                maxzoom: 11,
            });
            map.addLayer({
                id: "grid",
                type: "line",
                source: "grid",
                paint: {"line-color": colors.border, "line-width": 1},
            });
            LAYERS.forEach(
                ({id, sourceLayer, char, size, color, opacity, tooltip}) => {
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
                    map.on("mousemove", id, (e) => {
                        map.getCanvas().style.cursor = "pointer";
                        setTooltip({
                            x: e.point.x,
                            y: e.point.y,
                            ...tooltip(e.features[0].properties),
                        });
                    });
                    map.on("mouseleave", id, () => {
                        map.getCanvas().style.cursor = "";
                        setTooltip(null);
                    });
                },
            );
        });

        map.on("movestart", () => setTooltip(null));
        map.on("zoom", () => setZoom(map.getZoom()));
        map.on("mousemove", (e) =>
            setCursor({x: e.lngLat.lng, y: e.lngLat.lat}),
        );
        return () => map.remove();
    }, []);

    return (
        <div className="relative w-screen h-screen">
            <div ref={containerRef} className="w-full h-full"/>
            <Search mapRef={mapRef} setTooltip={setTooltip}/>
            <div className="absolute bottom-3 left-3 text-foreground font-mono text-base pointer-events-none">
                z {zoom.toFixed(2)} x {cursor.x.toFixed(4)} y{" "}
                {cursor.y.toFixed(4)}
            </div>
            {tooltip && (
                <div
                    className="absolute bg-overlay text-foreground font-mono text-base leading-normal py-1.5 px-2.5 whitespace-nowrap"
                    style={{left: tooltip.x + 12, top: tooltip.y + 12}}
                >
                    <div className="text-muted">{tooltip.entityType}</div>
                    <div className="italic">{tooltip.line1}</div>
                    <div>{tooltip.line2}</div>
                </div>
            )}
        </div>
    );
}
