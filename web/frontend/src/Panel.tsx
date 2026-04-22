import { useCallback, useEffect, useRef, useState } from "react";
import {
  AlbumSummary,
  ArtistSummary,
  LabelSummary,
  TrackSummary,
} from "./Summary.tsx";
import { displayTrackName, formatPlaylistCount } from "./utils.ts";
import { makeAbortable } from "./requests";
import type {
  AlbumInfo,
  AlbumRecommend,
  ArtistInfo,
  ArtistRecommend,
  Entity,
  EntityInfo,
  LabelInfo,
  LabelRecommend,
  Recommend,
  Selection,
  TrackInfo,
  TrackRecommend,
  UpdateFn,
} from "./types.ts";

type NavigateFn = (entity: Entity) => void;

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
              navigate({
                entity_type: "artist",
                rowid: s.artist_rowid,
                lon: s.artist_lon,
                lat: s.artist_lat,
                logcount: s.artist_logcount,
              })
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
              navigate({
                entity_type: "album",
                rowid: s.album_rowid,
                lon: s.album_lon,
                lat: s.album_lat,
                logcount: s.album_logcount,
              })
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
            navigate({
              entity_type: "label",
              rowid: s.label_rowid,
              lon: s.label_lon,
              lat: s.label_lat,
              logcount: s.label_logcount,
            })
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
              navigate({
                entity_type: "artist",
                rowid: s.artist_rowid,
                lon: s.artist_lon,
                lat: s.artist_lat,
                logcount: s.artist_logcount,
              })
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
            navigate({
              entity_type: "label",
              rowid: s.label_rowid,
              lon: s.label_lon,
              lat: s.label_lat,
              logcount: s.label_logcount,
            })
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
            <span key={r.rowid}>
              {i > 0 && <span className="text-muted mx-1">·</span>}
              <Link
                onClick={() => navigate(r)}
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
            <span key={r.rowid}>
              {i > 0 && <span className="text-muted mx-1">·</span>}
              <Link
                onClick={() => navigate(r)}
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
            <span key={r.rowid}>
              {i > 0 && <span className="text-muted mx-1">·</span>}
              <Link
                onClick={() => navigate(r)}
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

/** Returns display name and subtitle for a recommendation. */
function getRecDisplay(rec: Recommend): { name: string; sub: string } {
  const playlists = formatPlaylistCount(rec.logcount);
  switch (rec.entity_type) {
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
function RecItem({ rec, navigate }: { rec: Recommend; navigate: NavigateFn }) {
  const { name, sub } = getRecDisplay(rec);
  return (
    <li>
      <button
        onClick={(e) => {
          e.stopPropagation();
          navigate(rec);
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
  navigate,
}: {
  recs: Recommend[] | undefined;
  fetchStatus: FetchStatus;
  navigate: NavigateFn;
}) {
  if (recs) {
    if (recs.length === 0)
      return (
        <div className="text-muted text-xs py-2 px-4">No recommendations.</div>
      );
    return (
      <ol className="mt-1">
        {recs.map((rec) => (
          <RecItem
            key={`${rec.entity_type}:${rec.rowid}`}
            rec={rec}
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
  const rowid = entity.rowid;
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
      className={`block w-full cursor-pointer px-4 text-left text-xs 
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
      shadow-xl ui-no-pinch safe-bottom select-none"
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
          key={`${selection.entity_type}:${selection.rowid}`}
          entity={selection}
          navigate={navigate}
          update={update}
        />
      )}
    </div>
  );
}
