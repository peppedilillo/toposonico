import { useCallback, useEffect, useRef, useState } from "react";
import {
  AlbumSummary,
  ArtistSummary,
  LabelSummary,
  TrackSummary,
} from "./Summary.tsx";
import {
  displayTrackName,
  type EntityType,
  formatPlaylistCount,
  getRowid,
} from "./utils.ts";
import { makeAbortable } from "./requests";

// --- Types mirroring backend TypedDicts ---

type TrackInfo = {
  entity_type: "track";
  track_rowid: number;
  track_name_norm: string;
  artist_rowid: number;
  artist_name: string;
  album_rowid: number;
  album_name: string;
  album_name_norm: string;
  label_rowid: number;
  label: string;
  lon: number;
  lat: number;
  album_lon: number;
  album_lat: number;
  artist_lon: number;
  artist_lat: number;
  label_lon: number;
  label_lat: number;
  logcount: number;
  release_date: string | null;
};

type AlbumInfo = {
  entity_type: "album";
  album_rowid: number;
  album_name_norm: string;
  artist_rowid: number;
  artist_name: string;
  label_rowid: number;
  label: string;
  lon: number;
  lat: number;
  artist_lon: number;
  artist_lat: number;
  label_lon: number;
  label_lat: number;
  logcount: number;
  nrepr: number;
  total_tracks: number | null;
  release_date: string | null;
  album_type: string | null;
  reprs: TrackRepr[];
};

type ArtistInfo = {
  entity_type: "artist";
  artist_rowid: number;
  artist_name: string;
  lon: number;
  lat: number;
  logcount: number;
  ntrack: number;
  nalbum: number;
  nrepr: number;
  artist_genre: string | null;
  reprs: AlbumRepr[];
};

type LabelInfo = {
  entity_type: "label";
  label_rowid: number;
  label: string;
  lon: number;
  lat: number;
  logcount: number;
  ntrack: number;
  nalbum: number;
  nartist: number;
  nrepr: number;
  reprs: ArtistRepr[];
};

type EntityInfo = TrackInfo | AlbumInfo | ArtistInfo | LabelInfo;

// --- Recommendation types mirroring backend TypedDicts ---

type TrackRecommend = {
  track_rowid: number;
  track_name_norm: string;
  artist_name: string;
  lon: number;
  lat: number;
  logcount: number;
  simscore: number;
};

type AlbumRecommend = {
  album_rowid: number;
  album_name_norm: string;
  artist_name: string;
  lon: number;
  lat: number;
  logcount: number;
  simscore: number;
};

type ArtistRecommend = {
  artist_rowid: number;
  artist_name: string;
  lon: number;
  lat: number;
  logcount: number;
  simscore: number;
  artist_genre: string | null;
};

type LabelRecommend = {
  label_rowid: number;
  label: string;
  lon: number;
  lat: number;
  logcount: number;
  simscore: number;
};

type Recommend =
  | TrackRecommend
  | AlbumRecommend
  | ArtistRecommend
  | LabelRecommend;

// --- Repr types mirroring backend TypedDicts ---

type TrackRepr = {
  track_rowid: number;
  track_name_norm: string;
  artist_name: string;
  lon: number;
  lat: number;
};

type AlbumRepr = {
  album_rowid: number;
  album_name_norm: string;
  artist_name: string;
  lon: number;
  lat: number;
};

type ArtistRepr = {
  artist_rowid: number;
  artist_name: string;
  lon: number;
  lat: number;
};

/**
 * Discriminated union representing one entry in the navigation stack.
 * Loading/error keep entity identity so hash sync can stay declarative.
 */
export type Selection =
  | { status: "loading"; entity_type: EntityType; rowid: number }
  | { status: "error"; entity_type: EntityType; rowid: number }
  | ({ status: "loaded"; recs?: Recommend[] } & EntityInfo);

/** Shallow merge into the top of the nav stack when the target entity still matches. */
export type UpdateFn = (
  entityType: EntityType,
  rowid: number,
  patch: Partial<Selection & { recs: Recommend[] }>,
) => void;

type NavigateFn = (
  entityType: EntityType,
  rowid: number,
  lon: number,
  lat: number,
) => void;

type PanelProps = {
  selection: Selection | null;
  navigate: NavigateFn;
  update: UpdateFn;
  onClose: () => void;
  goBack: (() => void) | null;
};

// --- Internal sub-components ---

/** Navigable inline link styled with a per-entity color on hover. */
function Link({
  onClick,
  color,
  children,
  className,
}: {
  onClick: () => void;
  color: string;
  children: React.ReactNode;
  className: string;
}) {
  const [hovered, setHovered] = useState(false);

  return (
    <span
      role="button"
      tabIndex={0}
      onClick={(e) => {
        e.stopPropagation();
        onClick();
      }}
      onKeyDown={(e) => {
        if (e.key !== "Enter" && e.key !== " ") return;
        e.preventDefault();
        e.stopPropagation();
        onClick();
      }}
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
      style={hovered ? { color } : undefined}
      className={className}
    >
      {children}
    </span>
  );
}

const INLINE_LINK_CLASS = "inline cursor-pointer text-left transition-colors";

/** Horizontal scrollable row with wheel-to-scroll and gradient overflow fades. */
function ReprRow({ children }: { children: React.ReactNode }) {
  const ref = useRef<HTMLDivElement>(null);
  const [canScrollLeft, setCanScrollLeft] = useState(false);
  const [canScrollRight, setCanScrollRight] = useState(false);

  const updateFades = useCallback(() => {
    const el = ref.current;
    if (!el) return;
    setCanScrollLeft(el.scrollLeft > 0);
    setCanScrollRight(el.scrollLeft < el.scrollWidth - el.clientWidth - 1);
  }, []);

  // Handle Resize and Initial State
  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    updateFades();
    const observer = new ResizeObserver(updateFades);
    observer.observe(el);
    return () => observer.disconnect();
  }, [updateFades]);

  // Handles wheel events marking them as active.
  // These would be passive by default. Passive events can't call `preventDefault()`.
  // TODO: evaluate different solutions as this presently feels hackish.
  useEffect(() => {
    const el = ref.current;
    if (!el) return;

    const handleWheel = (e: WheelEvent) => {
      if (e.deltaY === 0) return;
      e.preventDefault();
      el.scrollLeft += e.deltaY; // noqa
      updateFades();
    };

    el.addEventListener("wheel", handleWheel, { passive: false });
    return () => {
      el.removeEventListener("wheel", handleWheel);
    };
  }, [updateFades]);

  const maskImage =
    canScrollLeft && canScrollRight
      ? "linear-gradient(to right, transparent, black 24px, black calc(100% - 24px), transparent)"
      : canScrollRight
        ? "linear-gradient(to right, black calc(100% - 24px), transparent)"
        : canScrollLeft
          ? "linear-gradient(to right, transparent, black 24px)"
          : undefined;

  return (
    <div
      ref={ref}
      onScroll={updateFades}
      className="overflow-x-auto whitespace-nowrap no-scrollbar text-sm mt-1"
      style={{ maskImage, WebkitMaskImage: maskImage }}
    >
      {children}
    </div>
  );
}

/** Animated placeholder shown while entity info is loading. */
function LoadingBody() {
  return (
    <div className="space-y-2 animate-pulse mt-1">
      <div className="h-5 bg-muted/20 rounded w-3/4" />
      <div className="h-4 bg-muted/20 rounded w-1/2" />
      <div className="h-3 bg-muted/20 rounded w-2/3" />
      <div className="h-3 bg-muted/20 rounded w-1/3" />
    </div>
  );
}

function TrackPanel({ s, navigate }: { s: TrackInfo; navigate: NavigateFn }) {
  return (
    <>
      <TrackSummary
        track={displayTrackName(s.track_name_norm)}
        artist={
          <Link
            onClick={() =>
              navigate("artist", s.artist_rowid, s.artist_lon, s.artist_lat)
            }
            color="var(--color-artist)"
            className={INLINE_LINK_CLASS}
          >
            {s.artist_name}
          </Link>
        }
        album={
          <Link
            onClick={() =>
              navigate("album", s.album_rowid, s.album_lon, s.album_lat)
            }
            color="var(--color-album)"
            className={INLINE_LINK_CLASS}
          >
            {s.album_name_norm}
          </Link>
        }
      />
      <div className="text-sm text-muted truncate">
        <Link
          onClick={() =>
            navigate("label", s.label_rowid, s.label_lon, s.label_lat)
          }
          color="var(--color-label)"
          className={INLINE_LINK_CLASS}
        >
          {s.label}
        </Link>
        {s.release_date && (
          <>
            {" "}
            · <span>{s.release_date.slice(0, 4)}</span>
          </>
        )}
      </div>
      <div className="text-sm text-muted mt-0.5 truncate">
        {formatPlaylistCount(s.logcount)}
      </div>
    </>
  );
}

function AlbumPanel({ s, navigate }: { s: AlbumInfo; navigate: NavigateFn }) {
  return (
    <>
      <AlbumSummary
        albumName={s.album_name_norm}
        artist={
          <Link
            onClick={() =>
              navigate("artist", s.artist_rowid, s.artist_lon, s.artist_lat)
            }
            color="var(--color-artist)"
            className={INLINE_LINK_CLASS}
          >
            {s.artist_name}
          </Link>
        }
      />
      <div className="text-sm text-muted truncate">
        <Link
          onClick={() =>
            navigate("label", s.label_rowid, s.label_lon, s.label_lat)
          }
          color="var(--color-label)"
          className={INLINE_LINK_CLASS}
        >
          {s.label}
        </Link>
        {s.release_date && (
          <>
            {" "}
            · <span>{s.release_date.slice(0, 4)}</span>
          </>
        )}
      </div>
      <div className="text-sm text-muted mt-0.5 truncate">
        {formatPlaylistCount(s.logcount)}
      </div>
      {s.reprs?.length > 0 && (
        <ReprRow>
          <span className="text-muted mr-1.5">features:</span>
          {s.reprs.map((r, i) => (
            <span key={r.track_rowid}>
              {i > 0 && <span className="text-muted mx-1">·</span>}
              <Link
                onClick={() => navigate("track", r.track_rowid, r.lon, r.lat)}
                color="var(--color-album)"
                className={INLINE_LINK_CLASS}
              >
                {displayTrackName(r.track_name_norm)}
              </Link>
            </span>
          ))}
        </ReprRow>
      )}
    </>
  );
}

function ArtistPanel({ s, navigate }: { s: ArtistInfo; navigate: NavigateFn }) {
  return (
    <>
      <ArtistSummary
        artistName={s.artist_name}
        genre={s.artist_genre ?? undefined}
        playlistCount={formatPlaylistCount(s.logcount)}
      />
      {s.reprs?.length > 0 && (
        <ReprRow>
          <span className="text-muted mr-1.5">top albums:</span>
          {s.reprs.map((r, i) => (
            <span key={r.album_rowid}>
              {i > 0 && <span className="text-muted mx-1">·</span>}
              <Link
                onClick={() => navigate("album", r.album_rowid, r.lon, r.lat)}
                color="var(--color-album)"
                className={INLINE_LINK_CLASS}
              >
                {r.album_name_norm}
              </Link>
            </span>
          ))}
        </ReprRow>
      )}
    </>
  );
}

function LabelPanel({ s, navigate }: { s: LabelInfo; navigate: NavigateFn }) {
  return (
    <>
      <LabelSummary
        labelName={s.label}
        playlistCount={formatPlaylistCount(s.logcount)}
      />
      {s.reprs?.length > 0 && (
        <ReprRow>
          <span className="text-muted mr-1.5">top artists:</span>
          {s.reprs.map((r, i) => (
            <span key={r.artist_rowid}>
              {i > 0 && <span className="text-muted mx-1">·</span>}
              <Link
                onClick={() => navigate("artist", r.artist_rowid, r.lon, r.lat)}
                color="var(--color-artist)"
                className={INLINE_LINK_CLASS}
              >
                {r.artist_name}
              </Link>
            </span>
          ))}
        </ReprRow>
      )}
    </>
  );
}

// --- Recommendation helpers and components ---

/** Extracts navigation args from a recommendation. */
function getRecNav(
  rec: Recommend,
  entityType: EntityType,
): [EntityType, number, number, number] {
  switch (entityType) {
    case "track": {
      const r = rec as TrackRecommend;
      return ["track", r.track_rowid, r.lon, r.lat];
    }
    case "album": {
      const r = rec as AlbumRecommend;
      return ["album", r.album_rowid, r.lon, r.lat];
    }
    case "artist": {
      const r = rec as ArtistRecommend;
      return ["artist", r.artist_rowid, r.lon, r.lat];
    }
    default: {
      const r = rec as LabelRecommend;
      return ["label", r.label_rowid, r.lon, r.lat];
    }
  }
}

/** Returns display name and subtitle for a recommendation. */
function getRecDisplay(
  rec: Recommend,
  entityType: EntityType,
): { name: string; sub: string } {
  const playlists = formatPlaylistCount(rec.logcount);
  switch (entityType) {
    case "track":
      return {
        name: displayTrackName((rec as TrackRecommend).track_name_norm),
        sub: (rec as TrackRecommend).artist_name + " · " + playlists,
      };
    case "album":
      return {
        name: (rec as AlbumRecommend).album_name_norm,
        sub: (rec as AlbumRecommend).artist_name + " · " + playlists,
      };
    case "artist": {
      const r = rec as ArtistRecommend;
      const parts = [r.artist_genre, playlists].filter(Boolean);
      return { name: r.artist_name, sub: parts.join(" · ") };
    }
    default:
      return { name: (rec as LabelRecommend).label, sub: playlists };
  }
}

/** Single recommendation row — full-width clickable button. */
function RecItem({
  rec,
  entityType,
  navigate,
}: {
  rec: Recommend;
  entityType: EntityType;
  navigate: NavigateFn;
}) {
  const { name, sub } = getRecDisplay(rec, entityType);
  const [et, rowid, lon, lat] = getRecNav(rec, entityType);
  return (
    <li>
      <button
        onClick={(e) => {
          e.stopPropagation();
          navigate(et, rowid, lon, lat);
        }}
        className="text-left w-full cursor-pointer hover:bg-overlay py-1.5 px-4"
      >
        <div className="text-sm font-medium truncate">{name}</div>
        <div className="text-xs text-muted truncate">{sub}</div>
      </button>
    </li>
  );
}

type FetchStatus = "idle" | "loading" | "error";

/** Renders loading/error/empty/list states for recommendations. */
function RecBody({
  recs,
  fetchStatus,
  entityType,
  navigate,
}: {
  recs: Recommend[] | undefined;
  fetchStatus: FetchStatus;
  entityType: EntityType;
  navigate: NavigateFn;
}) {
  if (recs) {
    if (recs.length === 0)
      return (
        <div className="text-muted text-xs py-2 px-4">No recommendations.</div>
      );
    return (
      <ol className="mt-1">
        {recs.map((rec, i) => (
          <RecItem
            key={i}
            rec={rec}
            entityType={entityType}
            navigate={navigate}
          />
        ))}
      </ol>
    );
  }
  if (fetchStatus === "error")
    return <div className="text-muted text-xs py-2 px-4">Failed to load.</div>;
  // fetchStatus is "loading": we update the "More like" button without filling RecsSection.
  return null;
}

/** A collapsible recommendations section, caching recommendations on the nav stack. */
function RecsSection({
  entity,
  navigate,
  update,
}: {
  entity: EntityInfo & { recs?: Recommend[] };
  navigate: NavigateFn;
  update: UpdateFn;
}) {
  const [open, setOpen] = useState(entity.recs != null);
  const [fetchStatus, setFetchStatus] = useState<FetchStatus>("idle");
  const aborter = useRef(makeAbortable());
  const rowid = getRowid(entity);
  const RECSNUMBER = 10;

  const handleToggle = () => {
    if (open) {
      setOpen(false);
      return;
    }
    setOpen(true);
    if (entity.recs != null || fetchStatus === "loading") return;
    setFetchStatus("loading");
    const signal = aborter.current.nextSignal();
    const popfloor = Math.max(Math.round(entity.logcount - 2), 0);
    fetch(
      `/api/recommend?rowid=${rowid}&entity_name=${entity.entity_type}&limit=${RECSNUMBER}&diverse=true&popfloor=${popfloor}`,
      { signal },
    )
      .then((r) => {
        if (!r.ok) throw new Error(r.statusText);
        return r.json();
      })
      .then((data: Recommend[]) => {
        update(entity.entity_type, rowid, { recs: data });
        setFetchStatus("idle");
      })
      .catch((err) => {
        if (err.name !== "AbortError") setFetchStatus("error");
      });
  };

  const trigger = (
    <button
      type="button"
      onClick={handleToggle}
      className={`block w-full cursor-pointer px-4 text-left text-xs select-none
      bg-linear-to-r from-gray-500 via-gray-50 to-gray-500 bg-size-[200%_auto] bg-clip-text text-transparent
      ${!open ? "animate-sweep" : ""}`}
    >
      {fetchStatus === "loading" ? "Loading..." : "More like this"}
    </button>
  );
  const hasVisibleRecs = open && entity.recs != null && entity.recs.length > 0;

  return (
    <div className="mt-3 border-t border-muted/20 pt-2 min-h-0 flex flex-col">
      {hasVisibleRecs ? (
        <div className="min-h-0 flex-1 overflow-y-auto overscroll-contain no-scrollbar">
          {trigger}
          <RecBody
            recs={entity.recs}
            fetchStatus={fetchStatus}
            entityType={entity.entity_type}
            navigate={navigate}
          />
        </div>
      ) : (
        <>
          {trigger}
          {open && (
            <RecBody
              recs={entity.recs}
              fetchStatus={fetchStatus}
              entityType={entity.entity_type}
              navigate={navigate}
            />
          )}
        </>
      )}
    </div>
  );
}

// --- Main Panel ---

/** Detail panel for a selected entity — bottom sheet on mobile, sidebar on desktop. */
export default function Panel({
  selection,
  navigate,
  update,
  onClose,
  goBack,
}: PanelProps) {
  if (!selection) return null;

  let body: React.ReactNode;
  if (selection.status === "loading") {
    body = <LoadingBody />;
  } else if (selection.status === "error") {
    body = <div className="text-muted text-sm mt-1">Failed to load.</div>;
  } else if (selection.entity_type === "track") {
    body = <TrackPanel s={selection} navigate={navigate} />;
  } else if (selection.entity_type === "album") {
    body = <AlbumPanel s={selection} navigate={navigate} />;
  } else if (selection.entity_type === "artist") {
    body = <ArtistPanel s={selection} navigate={navigate} />;
  } else {
    body = <LabelPanel s={selection} navigate={navigate} />;
  }

  return (
    <div
      className="panel z-10 flex flex-col overflow-hidden
      bg-surface font-sans text-base text-white
      shadow-xl ui-no-pinch safe-bottom"
      onPointerDown={(e) => e.stopPropagation()}
      onPointerMove={(e) => e.stopPropagation()}
    >
      <div className="relative px-4 pt-4 shrink-0">
        {body}
        <div className="absolute top-0 right-0 flex gap-1 items-center">
          {goBack && (
            <button
              onClick={goBack}
              className="text-muted hover:text-white transition-colors text-lg leading-none p-4"
              aria-label="Go back"
            >
              &lt;
            </button>
          )}
          <button
            onClick={onClose}
            className="text-muted hover:text-white transition-colors text-lg leading-none p-4"
            aria-label="Close"
          >
            ×
          </button>
        </div>
      </div>
      {selection.status === "loaded" && (
        <RecsSection
          key={`${selection.entity_type}:${getRowid(selection)}`}
          entity={selection}
          navigate={navigate}
          update={update}
        />
      )}
    </div>
  );
}
