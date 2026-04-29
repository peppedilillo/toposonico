import { useCallback, useEffect, useRef, useState } from "react";
import Search from "./Search.tsx";
import Panel from "./Panel.tsx";
import MapView, { type MapCommand, type ViewState } from "./MapView.tsx";
import { makeAbortable } from "./requests.ts";
import {
  ENTITY_BASE_ZOOMS,
  type EntityType,
  SOURCE_MAX_ZOOM,
} from "./utils.ts";
import Logo from "./assets/logo.svg";
import type { Entity, EntityInfo, Selection, UpdateFn } from "./types.ts";

const MAX_HISTORY = 20;

const INITIAL_VIEW: ViewState = {
  lon: 3.60,
  lat: -5.50,
  zoom: 5.00,
};

/** Returns true when the parsed hash entity matches one of the supported entities. */
function isEntityType(value: string | null): value is EntityType {
  return (
    value === "track" ||
    value === "album" ||
    value === "artist" ||
    value === "label"
  );
}

/** Reads map state from the URL hash. Missing keys fall back to INITIAL_VIEW defaults. */
function parseHash() {
  const params = new URLSearchParams(window.location.hash.slice(1));
  const lon = parseFloat(params.get("lon") ?? "");
  const lat = parseFloat(params.get("lat") ?? "");
  const zoom = parseFloat(params.get("z") ?? "");
  const rowid = parseInt(params.get("rowid") ?? "", 10);
  const entity = params.get("entity");

  return {
    view: {
      lon: Number.isNaN(lon) ? INITIAL_VIEW.lon : lon,
      lat: Number.isNaN(lat) ? INITIAL_VIEW.lat : lat,
      zoom: Number.isNaN(zoom) ? INITIAL_VIEW.zoom : zoom,
    },
    // invalid entity type and rowids in URL are dropped.
    entityType: isEntityType(entity) ? entity : null,
    rowid: Number.isNaN(rowid) ? null : rowid,
  };
}

/** Merges updates into the current URL hash. */
function updateHash(updates: Record<string, string | number | null>) {
  const params = new URLSearchParams(window.location.hash.slice(1));
  for (const [key, value] of Object.entries(updates)) {
    if (value == null) params.delete(key);
    else params.set(key, String(value));
  }
  history.replaceState(null, "", "#" + params.toString());
}

const ZOOM_PADDING = 0.25;

/** Root application component — owns navigation state, hash sync, and fetch coordination. */
export default function App() {
  const [initHash] = useState(parseHash);
  const [viewState, setViewState] = useState<ViewState>(initHash.view);
  const [stack, setStack] = useState<Selection[]>([]);
  const [mapCommand, setMapCommand] = useState<MapCommand>(null);
  const nextSelection = useRef(makeAbortable());
  const current = stack.length > 0 ? stack[stack.length - 1] : null;

  /** Flies the map to an entity, optionally applying an explicit zoom. */
  const flyToEntity = useCallback((entity: Entity, zoom?: number) => {
    setMapCommand({
      type: "flyTo",
      center: [entity.lon, entity.lat],
      zoom,
    });
  }, []);

  /** Zooms the map to the canonical per-entity zoom target without changing selection. */
  const zoomIn = useCallback(
    (entity: Entity) => {
      flyToEntity(
        entity,
        Math.min(
          SOURCE_MAX_ZOOM,
          ENTITY_BASE_ZOOMS[entity.entity_type] + ZOOM_PADDING,
        ),
      );
    },
    [flyToEntity],
  );

  /** Replaces the pending top entry when its identity still matches the finished request. */
  const resolveTop = useCallback((entity: Entity, next: Selection) => {
    setStack((prev) => {
      const top = prev[prev.length - 1];
      // without this guard, a late selection can pop over an already closed panel
      if (!top) return prev;
      // without this guard, select A and then B could still result in A being top of the stack
      if (top.status === "loaded") return prev;
      if (top.entity_type !== entity.entity_type) return prev;
      if (top.rowid !== entity.rowid) return prev;
      return [...prev.slice(0, -1), next];
    });
  }, []);

  /** Loads entity details for the current top-of-stack identity. */
  const loadSelection = useCallback(
    (entity: Entity) => {
      const signal = nextSelection.current.nextSignal();
      fetch(
        `/api/panel?rowid=${entity.rowid}&entity_name=${entity.entity_type}`,
        {
          signal,
        },
      )
        .then((response) => {
          if (!response.ok) throw new Error(response.statusText);
          return response.json();
        })
        .then((data: EntityInfo) => {
          resolveTop(entity, {
            status: "loaded",
            ...data,
          });
        })
        .catch((err) => {
          if (err.name !== "AbortError") {
            resolveTop(entity, {
              status: "error",
              ...entity,
            });
          }
        });
    },
    [resolveTop],
  );

  /** Fetches entity info and pushes it onto the nav stack, without moving the map. */
  const push = useCallback(
    (entity: Entity) => {
      setStack((prev) => {
        const pending: Selection = {
          status: "loading",
          ...entity,
        };
        const top = prev[prev.length - 1];
        if (!top || top.status === "loaded")
          return [...prev.slice(-(MAX_HISTORY - 1)), pending];
        // guarantees error/loading selection can only be on top of the stack
        return [...prev.slice(0, -1), pending];
      });
      loadSelection(entity);
    },
    [loadSelection],
  );

  /** Shallow-merges a patch into the current top entry when it still matches the target entity. */
  const update = useCallback<UpdateFn>((entityType, rowid, patch) => {
    setStack((prev) => {
      const index = prev.length - 1;
      if (index < 0) return prev;

      const selection = prev[index];
      // this protects against 1. attempting attaching recs to an unloaded Selection; and
      // 2. obsolete recs getting attached to latest selection in a race.
      if (selection.status !== "loaded") return prev;
      if (selection.entity_type !== entityType) return prev;
      if (selection.rowid !== rowid) return prev;

      return [...prev.slice(0, index), { ...selection, ...patch } as Selection];
    });
  }, []);

  /** Pops the top entry and flies back to the one beneath it (already loaded, no refetch). */
  const pop = useCallback(() => {
    if (stack.length < 2) return;

    nextSelection.current.cancel();
    const next = stack[stack.length - 2] as Extract<
      Selection,
      { status: "loaded" }
    >;
    setStack((prev) => prev.slice(0, -1));
    flyToEntity(next);
  }, [flyToEntity, stack]);

  /** Closes the panel and clears the nav stack. */
  const handlePanelClose = useCallback(() => {
    nextSelection.current.cancel();
    setStack([]);
  }, []);

  /** Recenters the map on an entity and pushes it onto the selection stack. */
  const navigate = useCallback(
    (entity: Entity) => {
      flyToEntity(entity);
      push(entity);
    },
    [flyToEntity, push],
  );

  /** Handles map feature clicks, which select without issuing a redundant fly-to. */
  const select = useCallback(
    (entity: Entity) => {
      push(entity);
    },
    [push],
  );

  /** Fetches entity info from URL hash on initial mount, without moving the map. */
  useEffect(() => {
    if (!initHash.entityType || initHash.rowid == null) return;
    const signal = nextSelection.current.nextSignal();
    fetch(
      `/api/panel?rowid=${initHash.rowid}&entity_name=${initHash.entityType}`,
      {
        signal,
      },
    )
      .then((response) => {
        if (!response.ok) throw new Error(response.statusText);
        return response.json();
      })
      .then((data: EntityInfo) => {
        setStack((prev) => {
          if (prev.length > 0) return prev;
          return [{ status: "loaded", ...data }];
        });
      })
      .catch(() => {});
  }, [initHash.entityType, initHash.rowid]);

  /** Debounced URL hash sync — avoids iOS Safari replaceState rate-limit during flyTo. */
  const hashEntity = current == null ? null : current.entity_type;

  const hashRowid = current?.rowid ?? null;

  useEffect(() => {
    const timer = setTimeout(() => {
      updateHash({
        lon: viewState.lon.toFixed(4),
        lat: viewState.lat.toFixed(4),
        z: viewState.zoom.toFixed(2),
        entity: hashEntity,
        rowid: hashRowid,
      });
    }, 100);

    return () => clearTimeout(timer);
  }, [hashEntity, hashRowid, viewState.lat, viewState.lon, viewState.zoom]);

  return (
    <div className="relative w-screen h-screen">
      <MapView
        initialView={initHash.view}
        command={mapCommand}
        onMoveEnd={setViewState}
        onFeatureSelect={select}
      />
      <Search navigate={navigate} panelOpen={current != null} />
      <Panel
        selection={current}
        navigate={navigate}
        zoomIn={current?.status === "loaded" ? () => zoomIn(current) : null}
        update={update}
        onClose={handlePanelClose}
        goBack={stack.length > 1 ? pop : null}
      />
      <img
        src={Logo}
        alt="Toposonico"
        className="fixed z-5 sm:w-6 w-6 pointer-events-none
                   bottom-[var(--ui-edge-gap)] right-[var(--ui-edge-gap)] select-none"
      />
    </div>
  );
}
