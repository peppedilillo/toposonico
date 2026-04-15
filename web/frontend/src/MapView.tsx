import { useEffect, useRef } from "react";
import maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import type { FeatureCollection } from "geojson";
import type { ExpressionSpecification } from "maplibre-gl";
import type { EntityType } from "./utils.ts";

export type ViewState = {
  lon: number;
  lat: number;
  zoom: number;
};

export type MapCommand = null | {
  type: "flyTo";
  center: [number, number];
  zoom?: number;
};

type MapViewProps = {
  initialView: ViewState;
  command: MapCommand;
  onMoveEnd: (view: ViewState) => void;
  onFeatureSelect: (entityType: EntityType, rowid: number) => void;
};

const VIEW_CONSTRAINTS = {
  minZoom: 5,
  maxZoom: 14,
  pitch: 10,
};

const MIN_VISUAL_RADIUS = 1;
const MIN_HIT_RADIUS = 11;
const HIT_STROKE_OPACITY = 0.003;
const HIT_FILL_OPACITY = 0.004;
const INTERACTIVE_LAYER_IDS = ["tracks", "labels-hit", "artists-hit", "albums"];

// MapLibre requires absolute URLs for tile sources. In dev the env var is a relative path
// and we prepend the page origin so it works on both localhost and LAN.
const TILE_URL_RAW =
  (import.meta.env.VITE_TILES_URL as string | undefined) ??
  "/tiles/{z}/{x}/{y}.pbf";
const TILE_URL = TILE_URL_RAW.startsWith("http")
  ? TILE_URL_RAW
  : window.location.origin + TILE_URL_RAW;

/** Reads a CSS custom property value (e.g. "#3bda28") from :root. */
function cssVar(name: string): string {
  return getComputedStyle(document.documentElement)
    .getPropertyValue(name)
    .trim();
}

/** 5-degree lat/lon graticule rendered below entity layers. */
const GRID_LINES: FeatureCollection = {
  type: "FeatureCollection",
  features: [],
};
for (let lon = -180; lon <= 180; lon += 5) {
  GRID_LINES.features.push({
    type: "Feature",
    geometry: {
      type: "LineString",
      coordinates: [
        [lon, -90],
        [lon, 90],
      ],
    },
    properties: {},
  });
}
for (let lat = -90; lat <= 90; lat += 5) {
  GRID_LINES.features.push({
    type: "Feature",
    geometry: {
      type: "LineString",
      coordinates: [
        [-180, lat],
        [180, lat],
      ],
    },
    properties: {},
  });
}

/** Parses a feature into a number. */
function parseNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

/** Base visual radius shared by all entity layers. */
function getVisualRadiusExpression() {
  return ["max", MIN_VISUAL_RADIUS, ["get", "logcount"]] as ExpressionSpecification;
}

/** Stroke width needed to pad a visible circle out to the minimum hit radius. */
function getHitStrokeWidthExpression() {
  return ["max", 0, ["-", MIN_HIT_RADIUS, getVisualRadiusExpression()]] as ExpressionSpecification;
}

/** Radius used by ghost layers so small circles remain easy to hit. */
function getHitRadiusExpression() {
  return ["max", MIN_HIT_RADIUS, ["get", "logcount"]] as ExpressionSpecification;
}

/** MapLibre wrapper responsible only for map rendering and imperative camera commands. */
export default function MapView({
  initialView,
  command,
  onMoveEnd,
  onFeatureSelect,
}: MapViewProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const initialViewRef = useRef(initialView);
  const onFeatureSelectRef = useRef(onFeatureSelect);
  const onMoveEndRef = useRef(onMoveEnd);

  /** Keep callback refs in sync so the map's event handlers always call the latest props. */
  useEffect(() => {
    onFeatureSelectRef.current = onFeatureSelect;
    onMoveEndRef.current = onMoveEnd;
  });

  useEffect(() => {
    const { lon, lat, zoom } = initialViewRef.current;
    const map = new maplibregl.Map({
      container: containerRef.current!,
      style: {
        version: 8,
        sources: {},
        layers: [
          {
            id: "background",
            type: "background",
            paint: { "background-color": "#0d0d12" },
          },
        ],
      },
      center: [lon, lat],
      zoom,
      minZoom: VIEW_CONSTRAINTS.minZoom,
      maxZoom: VIEW_CONSTRAINTS.maxZoom,
      pitch: VIEW_CONSTRAINTS.pitch,
      renderWorldCopies: false,
      attributionControl: false,
    });
    mapRef.current = map;

    map.on("load", () => {
      map.addSource("grid", { type: "geojson", data: GRID_LINES });
      map.addLayer({
        id: "grid",
        type: "line",
        source: "grid",
        paint: {
          "line-color": cssVar("--color-border"),
          "line-width": 1,
        },
      });

      map.addSource("entities", {
        type: "vector",
        tiles: [TILE_URL],
        minzoom: VIEW_CONSTRAINTS.minZoom,
        maxzoom: VIEW_CONSTRAINTS.maxZoom,
      });

      // there are much more tracks than any other entity class, we place them at z-order bottom
      map.addLayer({
        id: "tracks",
        type: "circle",
        source: "entities",
        "source-layer": "tracks",
        paint: {
          "circle-radius": getVisualRadiusExpression(),
          "circle-color": cssVar("--color-track"),
          "circle-opacity": 0.5,
          // Outer stroke pads the hit target so tiny track dots stay clickable.
          "circle-stroke-color": cssVar("--color-track"),
          "circle-stroke-opacity": HIT_STROKE_OPACITY,
          "circle-stroke-width": getHitStrokeWidthExpression(),
        },
      });
      map.addLayer({
        id: "labels",
        type: "circle",
        source: "entities",
        "source-layer": "labels",
        paint: {
          "circle-radius": getVisualRadiusExpression(),
          "circle-color": "transparent",
          "circle-stroke-color": cssVar("--color-label"),
          "circle-stroke-opacity": 0.7,
          "circle-stroke-width": 1,
        },
      });
      // Ghost hit layer keeps small label outlines easy to tap without changing the visible stroke.
      map.addLayer({
        id: "labels-hit",
        type: "circle",
        source: "entities",
        "source-layer": "labels",
        paint: {
          "circle-radius": getHitRadiusExpression(),
          "circle-color": cssVar("--color-label"),
          "circle-opacity": HIT_FILL_OPACITY,
        },
      });
      // Ghost hit layer keeps small artist outlines easy to tap without changing the visible stroke.
      map.addLayer({
        id: "artists-hit",
        type: "circle",
        source: "entities",
        "source-layer": "artists",
        paint: {
          "circle-radius": getHitRadiusExpression(),
          "circle-color": cssVar("--color-artist"),
          "circle-opacity": HIT_FILL_OPACITY,
        },
      });
      map.addLayer({
        id: "artists",
        type: "circle",
        source: "entities",
        "source-layer": "artists",
        paint: {
          "circle-radius": getVisualRadiusExpression(),
          "circle-color": "transparent",
          "circle-stroke-color": cssVar("--color-artist"),
          "circle-stroke-opacity": 0.7,
          "circle-stroke-width": 1,
        },
      });
      map.addLayer({
        id: "albums",
        type: "circle",
        source: "entities",
        "source-layer": "albums",
        paint: {
          "circle-radius": getVisualRadiusExpression(),
          "circle-color": cssVar("--color-album"),
          "circle-opacity": 0.6,
          // Outer stroke pads the hit target so tiny album dots stay clickable.
          "circle-stroke-color": cssVar("--color-album"),
          "circle-stroke-opacity": HIT_STROKE_OPACITY,
          "circle-stroke-width": getHitStrokeWidthExpression(),
        },
      });

      map.on("click", (e) => {
        const features = map.queryRenderedFeatures(e.point, {
          layers: INTERACTIVE_LAYER_IDS,
        });

        let bestSelection: {
          entityType: EntityType;
          rowid: number;
          logcount: number;
        } | null = null;

        for (const feature of features) {
          let entityType: EntityType;
          let rowid: number | null;

          switch (feature.layer.id) {
            case "artists-hit":
              entityType = "artist";
              rowid = parseNumber(feature.properties?.artist_rowid);
              break;
            case "albums":
              entityType = "album";
              rowid = parseNumber(feature.properties?.album_rowid);
              break;
            case "tracks":
              entityType = "track";
              rowid = parseNumber(feature.properties?.track_rowid);
              break;
            case "labels-hit":
              entityType = "label";
              rowid = parseNumber(feature.properties?.label_rowid);
              break;
            default:
              continue;
          }

          const logcount = parseNumber(feature.properties?.logcount);
          if (rowid == null || logcount == null) continue;
          if (bestSelection != null && logcount <= bestSelection.logcount) continue;

          bestSelection = { entityType, rowid, logcount };
        }

        if (bestSelection != null) {
          onFeatureSelectRef.current(bestSelection.entityType, bestSelection.rowid);
        }
      });

      map.on("mousemove", (e) => {
        const hasInteractiveFeature =
          map.queryRenderedFeatures(e.point, { layers: INTERACTIVE_LAYER_IDS }).length >
          0;
        map.getCanvas().style.cursor = hasInteractiveFeature ? "pointer" : "";
      });

      map.on("mouseleave", () => {
        map.getCanvas().style.cursor = "";
      });
    });

    map.on("moveend", () => {
      const center = map.getCenter();
      onMoveEndRef.current({
        lon: center.lng,
        lat: center.lat,
        zoom: map.getZoom(),
      });
    });

    return () => map.remove();
    // Runs once on mount; initialView and callbacks are read via refs.
  }, []);

  useEffect(() => {
    if (!command || command.type !== "flyTo") return;
    mapRef.current?.flyTo({ center: command.center, zoom: command.zoom });
  }, [command]);

  return <div ref={containerRef} className="w-full h-full select-none" />;
}
