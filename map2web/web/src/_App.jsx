import { useEffect, useRef, useState } from "react";
import maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";

const TILE_URL = `${window.location.origin}/tiles/{z}/{x}/{y}.pbf`;

const STYLE = {
    version: 8,
    sources: {},
    glyphs: "/fonts/IBMPlexMono/{fontstack}/{range}.pbf",
    layers: [
        {
            id: "background",
            type: "background",
            paint: { "background-color": "#0d0d12" },
        },
    ],
};

function makeMarkerImage(map, name, drawFn, size = 20) {
    const canvas = document.createElement("canvas");
    canvas.width = size;
    canvas.height = size;
    const ctx = canvas.getContext("2d");
    ctx.clearRect(0, 0, size, size);
    ctx.fillStyle = "white";
    drawFn(ctx, size);
    const { data } = ctx.getImageData(0, 0, size, size);
    map.addImage(name, { width: size, height: size, data }, { sdf: true });
}

const MARKER_PAINT = {
    "icon-color": [
        "interpolate",
        ["linear"],
        ["get", "logcounts"],
        1,
        "#0b4301",
        2,
        "#1f8700",
        3,
        "#1ec501",
        4,
        "#4ed500",
        5,
        "#88ff00",
    ],
    "icon-opacity": [
        "interpolate",
        ["linear"],
        ["zoom"],
        0,
        0.4,
        0.001,
        0.6,
        0.01,
        0.85,
    ],
};

function markerLayout(imageId) {
    return {
        "icon-image": imageId,
        "icon-size": [
            "interpolate",
            ["linear"],
            ["zoom"],
            1,
            [
                "interpolate",
                ["linear"],
                ["get", "logcounts"],
                0,
                0.1,
                100,
                0.25,
            ],
            2,
            [
                "interpolate",
                ["linear"],
                ["get", "logcounts"],
                0,
                0.15,
                100,
                0.4,
            ],
            4,
            ["interpolate", ["linear"], ["get", "logcounts"], 0, 0.2, 100, 0.6],
            8,
            ["interpolate", ["linear"], ["get", "logcounts"], 0, 0.3, 100, 0.9],
        ],
        "icon-allow-overlap": true,
        "icon-ignore-placement": true,
        visibility: "visible",
    };
}

const SYMBOL_PAINT = {
    "text-color": "#ffffff",
    "text-halo-color": "rgba(6, 8, 16, 0.95)",
    "text-halo-width": 1.2,
};

const SYMBOL_LAYOUT = {
    "text-font": ["IBM Plex Mono Regular"],
    "text-size": 11,
    "text-offset": [0, 1.2],
    "text-anchor": "top",
    "text-max-width": 12,
};

const ENTITIES = [
    {
        id: "tracks",
        label: "Tracks",
        typeLabel: "TRACK",
        markerImage: "marker-circle",
        sourceLayer: "tracks",
        circleMinzoom: 5,
        labelMinzoom: 6,
        textField: [
            "format",
            ["get", "artist_name"],
            { "font-scale": 0.85 },
            "\n",
            {},
            ["get", "track_name"],
            { "font-scale": 1 },
        ],
        labelFilter: [
            ">",
            ["get", "logcounts"],
            ["step", ["zoom"], 70, 7, 60, 8, 55, 9, 50],
        ],
        tipLine1: (p) => p.artist_name,
        tipLine2: (p) => p.track_name,
    },
    {
        id: "albums",
        label: "Albums",
        typeLabel: "ALBUM",
        markerImage: "marker-square",
        sourceLayer: "albums",
        circleMinzoom: 2,
        labelMinzoom: 3,
        textField: [
            "format",
            ["get", "artist_name"],
            { "font-scale": 0.85 },
            "\n",
            {},
            ["get", "album_name"],
            { "font-scale": 1 },
        ],
        labelFilter: [
            ">",
            ["get", "logcounts"],
            ["step", ["zoom"], 70, 7, 60, 8, 55, 9, 50],
        ],
        tipLine1: (p) => p.artist_name,
        tipLine2: (p) => p.album_name,
    },
    {
        id: "artists",
        label: "Artists",
        typeLabel: "ARTIST",
        markerImage: "marker-triangle",
        sourceLayer: "artists",
        circleMinzoom: 1,
        labelMinzoom: 2,
        textField: ["get", "artist_name"],
        labelFilter: null,
        tipLine1: (p) => p.artist_name,
        tipLine2: (p) => `${p.track_count} tracks`,
    },
    {
        id: "labels",
        label: "Labels",
        typeLabel: "LABEL",
        markerImage: "marker-diamond",
        sourceLayer: "labels",
        circleMinzoom: 0,
        labelMinzoom: 1,
        textField: ["get", "label"],
        labelFilter: null,
        tipLine1: (p) => p.label,
        tipLine2: (p) => `${p.track_count} tracks`,
    },
];

export default function App() {
    const containerRef = useRef(null);
    const mapRef = useRef(null);
    const [tooltip, setTooltip] = useState(null); // {x, y, entityType, line1, line2}
    const [zoom, setZoom] = useState(null);

    useEffect(() => {
        const map = new maplibregl.Map({
            container: containerRef.current,
            style: STYLE,
            center: [0, 0],
            minZoom: 3,
        });
        mapRef.current = map;

        map.on("zoom", () => setZoom(map.getZoom()));
        map.on("load", () => {
            setZoom(map.getZoom());
            map.addSource("tiles", {
                type: "vector",
                tiles: [TILE_URL],
                maxzoom: 11,
            });

            makeMarkerImage(map, "marker-circle", (ctx, s) => {
                ctx.beginPath();
                ctx.arc(s / 2, s / 2, s / 2 - 2, 0, 2 * Math.PI);
                ctx.fill();
            });
            makeMarkerImage(map, "marker-square", (ctx, s) => {
                ctx.fillRect(2, 2, s - 4, s - 4);
            });
            makeMarkerImage(map, "marker-diamond", (ctx, s) => {
                ctx.beginPath();
                ctx.moveTo(s / 2, 1);
                ctx.lineTo(s - 1, s / 2);
                ctx.lineTo(s / 2, s - 1);
                ctx.lineTo(1, s / 2);
                ctx.closePath();
                ctx.fill();
            });
            makeMarkerImage(map, "marker-triangle", (ctx, s) => {
                ctx.beginPath();
                ctx.moveTo(s / 2, 1);
                ctx.lineTo(s - 1, s - 1);
                ctx.lineTo(1, s - 1);
                ctx.closePath();
                ctx.fill();
            });

            map.addLayer({
                id: "labels",
                type: "symbol",
                source: "tiles",
                "source-layer": "labels",
                minzoom: 1,
                paint: MARKER_PAINT,
                layout: markerLayout("marker-diamond"),
            });
            map.addLayer({
                id: "artists",
                type: "symbol",
                source: "tiles",
                "source-layer": "artists",
                minzoom: 1,
                paint: MARKER_PAINT,
                layout: markerLayout("marker-triangle"),
            });
            map.addLayer({
                id: "albums",
                type: "symbol",
                source: "tiles",
                "source-layer": "albums",
                minzoom: 1,
                paint: MARKER_PAINT,
                layout: markerLayout("marker-square"),
            });
            map.addLayer({
                id: "tracks",
                type: "symbol",
                source: "tiles",
                "source-layer": "tracks",
                minzoom: 1,
                paint: MARKER_PAINT,
                layout: markerLayout("marker-circle"),
            });

            map.addLayer({
                id: "labels-labels",
                type: "symbol",
                source: "tiles",
                "source-layer": "labels",
                minzoom: 5,
                filter: [
                    ">",
                    ["get", "logcounts"],
                    ["step", ["zoom"], 1.0, 6, 0.9, 7, 0.8],
                ],
                layout: {
                    "text-field": ["get", "label"],
                    ...SYMBOL_LAYOUT,
                    visibility: "visible",
                },
                paint: SYMBOL_PAINT,
            });
            map.addLayer({
                id: "artists-labels",
                type: "symbol",
                source: "tiles",
                "source-layer": "artists",
                minzoom: 5,
                filter: [
                    ">",
                    ["get", "logcounts"],
                    ["step", ["zoom"], 1.25, 6, 1.0],
                ],
                layout: {
                    "text-field": [
                        "format",
                        ["get", "artist_name"],
                        { "font-scale": 0.85 },
                    ],
                    ...SYMBOL_LAYOUT,
                    visibility: "none",
                },
                paint: SYMBOL_PAINT,
            });
            map.addLayer({
                id: "albums-labels",
                type: "symbol",
                source: "tiles",
                "source-layer": "albums",
                minzoom: 5,
                filter: [
                    ">",
                    ["get", "logcounts"],
                    [
                        "step",
                        ["zoom"],
                        2.5,
                        6,
                        2.45,
                        7,
                        2.4,
                        8,
                        2.35,
                        9,
                        2.3,
                        10,
                        2.25,
                        11,
                        2.2,
                    ],
                ],
                layout: {
                    "text-field": [
                        "format",
                        ["get", "artist_name"],
                        { "font-scale": 0.8 },
                        "\n",
                        {},
                        ["get", "album_name"],
                        { "font-scale": 1 },
                    ],
                    ...SYMBOL_LAYOUT,
                    visibility: "visible",
                },
                paint: SYMBOL_PAINT,
            });
            map.addLayer({
                id: "tracks-labels",
                type: "symbol",
                source: "tiles",
                "source-layer": "tracks",
                minzoom: 10,
                filter: [
                    ">",
                    ["get", "logcounts"],
                    ["step", ["zoom"], 70, 11, 60],
                ],
                layout: {
                    "text-field": [
                        "format",
                        ["get", "artist_name"],
                        { "font-scale": 0.85 },
                        "\n",
                        {},
                        ["get", "track_name"],
                        { "font-scale": 1 },
                    ],
                    ...SYMBOL_LAYOUT,
                    visibility: "none",
                },
                paint: SYMBOL_PAINT,
            });

            ENTITIES.forEach(({ id, typeLabel, tipLine1, tipLine2 }) => {
                map.on("mousemove", id, (e) => {
                    map.getCanvas().style.cursor = "pointer";
                    const props = e.features[0].properties;
                    setTooltip({
                        x: e.point.x,
                        y: e.point.y,
                        entityType: typeLabel,
                        line1: tipLine1(props),
                        line2: tipLine2(props),
                        logcounts: props.logcounts,
                    });
                });
                map.on("mouseleave", id, () => {
                    map.getCanvas().style.cursor = "";
                    setTooltip(null);
                });
            });
        });

        return () => map.remove();
    }, []);

    return (
        <div style={{ position: "relative", width: "100vw", height: "100vh" }}>
            <div ref={containerRef} style={{ width: "100%", height: "100%" }} />
            {zoom !== null && (
                <div
                    style={{
                        position: "absolute",
                        bottom: 12,
                        left: 12,
                        pointerEvents: "none",
                        background: "rgba(13,13,18,0.75)",
                        color: "rgba(220,230,255,0.6)",
                        fontFamily: "monospace",
                        fontSize: "11px",
                        padding: "3px 7px",
                    }}
                >
                    z {zoom.toFixed(2)}
                </div>
            )}
            {tooltip && (
                <div
                    style={{
                        position: "absolute",
                        left: tooltip.x + 12,
                        top: tooltip.y + 12,
                        pointerEvents: "none",
                        background: "rgba(13, 13, 18, 0.85)",
                        color: "rgba(220, 230, 255, 0.9)",
                        fontFamily: "monospace",
                        fontSize: "12px",
                        lineHeight: "1.5",
                        padding: "6px 10px",
                        whiteSpace: "nowrap",
                    }}
                >
                    <div
                        style={{
                            fontSize: "9px",
                            letterSpacing: "0.1em",
                            opacity: 0.45,
                            marginBottom: 2,
                        }}
                    >
                        {tooltip.entityType}
                    </div>
                    <div style={{ opacity: 0.6 }}>{tooltip.line1}</div>
                    <div
                        style={
                            tooltip.entityType === "LABEL"
                                ? { fontStyle: "italic" }
                                : {}
                        }
                    >
                        {tooltip.line2}
                    </div>
                    <div style={{ opacity: 0.45, marginTop: 2 }}>
                        pop {tooltip.logcounts}
                    </div>
                </div>
            )}
        </div>
    );
}
