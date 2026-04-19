import { useCallback, useEffect, useRef, useState } from "react";
import Search from "./Search.tsx";
import Panel from "./Panel.tsx";
import MapView, { type MapCommand, type ViewState } from "./MapView.tsx";
import { makeAbortable } from "./requests.ts";
import { type EntityType, getRowid } from "./utils.ts";
import Logo from "./assets/logo.svg";
import type { Selection, UpdateFn } from "./Panel.tsx";

const MAX_HISTORY = 20;

const INITIAL_VIEW: ViewState = {
  lon: 0.,
  lat: -4.,
  zoom: 7.,
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

/** Root application component — owns navigation state, hash sync, and fetch coordination. */
export default function App() {
  const [initHash] = useState(parseHash);
  const [viewState, setViewState] = useState<ViewState>(initHash.view);
  const [stack, setStack] = useState<Selection[]>(() =>
    initHash.entityType && initHash.rowid != null
      ? [
          {
            status: "loading",
            entity_type: initHash.entityType,
            rowid: initHash.rowid,
          },
        ]
      : [],
  );
  const [mapCommand, setMapCommand] = useState<MapCommand>(null);
  const nextSelection = useRef(makeAbortable());
  const current = stack.length > 0 ? stack[stack.length - 1] : null;

  /** Replaces the pending top entry when its identity still matches the finished request. */
  const resolveTop = useCallback(
    (entityType: EntityType, rowid: number, next: Selection) => {
      setStack((prev) => {
        const top = prev[prev.length - 1];
        // without this guard, a late selection can pop over an already closed panel
        if (!top) return prev;
        // without this guard, select A and then B could still result in A being top of the stack
        if (top.status === "loaded") return prev;
        if (top.entity_type !== entityType) return prev;
        if (top.rowid !== rowid) return prev;
        return [...prev.slice(0, -1), next];
      });
    },
    [],
  );

  /** Loads entity details for the current top-of-stack identity. */
  const loadSelection = useCallback(
    (entityType: EntityType, rowid: number) => {
      const signal = nextSelection.current.nextSignal();
      fetch(`/api/panel?rowid=${rowid}&entity_name=${entityType}`, { signal })
        .then((response) => {
          if (!response.ok) throw new Error(response.statusText);
          return response.json();
        })
        .then((data) => {
          resolveTop(entityType, rowid, {
            status: "loaded",
            entity_type: entityType,
            ...data,
          });
        })
        .catch((err) => {
          if (err.name !== "AbortError") {
            resolveTop(entityType, rowid, {
              status: "error",
              entity_type: entityType,
              rowid,
            });
          }
        });
    },
    [resolveTop],
  );

  /** Fetches entity info and pushes it onto the nav stack, without moving the map. */
  const push = useCallback(
    (entityType: EntityType, rowid: number) => {
      setStack((prev) => {
        const pending: Selection = {
          status: "loading",
          entity_type: entityType,
          rowid,
        };
        const top = prev[prev.length - 1];
        if (!top || top.status === "loaded")
          return [...prev.slice(-(MAX_HISTORY - 1)), pending];
        // guarantees error/loading selection can only be on top of the stack
        return [...prev.slice(0, -1), pending];
      });
      loadSelection(entityType, rowid);
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
      if (getRowid(selection) !== rowid) return prev;

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
    setMapCommand({
      type: "flyTo",
      center: [next.lon, next.lat],
    });
  }, [stack]);

  /** Closes the panel and clears the nav stack. */
  const handlePanelClose = useCallback(() => {
    nextSelection.current.cancel();
    setStack([]);
  }, []);

  /** Flies the map to the given coordinates and pushes a new selection. */
  const navigate = useCallback(
    (entityType: EntityType, rowid: number, lon: number, lat: number) => {
      setMapCommand({
        type: "flyTo",
        center: [lon, lat],
      });
      push(entityType, rowid);
    },
    [push],
  );

  /** Handles map feature clicks, which select without issuing a redundant fly-to. */
  const select = useCallback(
    (entityType: EntityType, rowid: number) => {
      push(entityType, rowid);
    },
    [push],
  );

  /** Fetches entity info from URL hash on initial mount, without moving the map. */
  useEffect(() => {
    if (!initHash.entityType || initHash.rowid == null) return;
    loadSelection(initHash.entityType, initHash.rowid);
  }, [initHash.entityType, initHash.rowid, loadSelection]);

  /** Debounced URL hash sync — avoids iOS Safari replaceState rate-limit during flyTo. */
  const hashEntity = current == null ? null : current.entity_type;

  const hashRowid =
    current == null
      ? null
      : current.status === "loaded"
        ? getRowid(current)
        : current.rowid;

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
        update={update}
        onClose={handlePanelClose}
        goBack={stack.length > 1 ? pop : null}
      />
      <img
        src={Logo}
        alt="Hummap"
        className="fixed z-5 sm:w-6 w-6 pointer-events-none
                   bottom-[var(--ui-edge-gap)] right-[var(--ui-edge-gap)] select-none"
      />
    </div>
  );
}
