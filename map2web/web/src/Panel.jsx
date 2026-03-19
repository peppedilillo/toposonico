import { useEffect, useState } from "react";
import { Badge } from "./Badge.jsx";
import colors from "./theme.js";

export function Link({ onClick, children, color }) {
    const [hovered, setHovered] = useState(false);
    return (
        <button
            onClick={onClick}
            onMouseEnter={() => setHovered(true)}
            onMouseLeave={() => setHovered(false)}
            style={hovered && color ? { color } : undefined}
            className="cursor-pointer text-left max-w-full truncate transition-colors"
        >
            {children}
        </button>
    );
}

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

// labels use string `label_id` as PK; all other entities use integer `<entity>_rowid`
const ROWID_KEY = {
    track: "track_rowid", album: "album_rowid",
    artist: "artist_rowid", label: "label_id",
};
const getRowid = (entityType, obj) => obj[ROWID_KEY[entityType]];

const LINE1 = {
    track:  r => r.track_name,
    album:  r => r.album_name,
    artist: r => r.artist_name,
    label:  r => r.label,
};
const LINE2 = {
    track:  r => r.artist_name,
    album:  r => r.artist_name,
    artist: r => `${r.track_count} tracks`,
    label:  r => `${r.track_count} tracks`,
};

function RecItem({ rec, entityType, navigate }) {
    return (
        <li className="hover:bg-overlay -mx-5 px-5 pt-2">
            <button
                onClick={() => navigate(entityType, getRowid(entityType, rec), rec.lon, rec.lat)}
                className="text-left w-full cursor-pointer"
            >
                <div className="text-sm font-medium truncate">{LINE1[entityType](rec)}</div>
                <div className="text-xs text-muted truncate">{LINE2[entityType](rec)}</div>
            </button>
        </li>
    );
}

function RecsList({ recs, entityType, navigate }) {
    if (!recs || recs.loading)
        return <div className="text-muted text-xs py-2 animate-pulse">Loading…</div>;
    if (recs.error)
        return <div className="text-muted text-xs py-2">Failed to load.</div>;
    if (!recs.length)
        return <div className="text-muted text-xs py-2">No recommendations.</div>;
    return (
        <ol className="mt-1">
            {recs.map((rec, i) => (
                <RecItem key={i} rec={rec} entityType={entityType} navigate={navigate} />
            ))}
        </ol>
    );
}

function TrackPanel({ s, navigate }) {
    return (
        <>
            <div className="text-lg font-semibold leading-snug truncate">{s.track_name}</div>
            <div className="italic font-medium leading-tight text-sm mt-0.5 truncate">
                {s.artist_lon != null ? (
                    <Link onClick={() => navigate("artist", s.artist_rowid, s.artist_lon, s.artist_lat)} color={colors.artist}>
                        {s.artist_name}
                    </Link>
                ) : s.artist_name}
            </div>
            {s.album_name && (
                <div className="text-sm mt-1 truncate">
                    {s.album_lon != null ? (
                        <Link onClick={() => navigate("album", s.album_rowid, s.album_lon, s.album_lat)} color={colors.album}>
                            {s.album_name}
                        </Link>
                    ) : s.album_name}
                </div>
            )}
            {s.label && (
                <div className="text-xs text-muted mt-0.5 truncate">
                    {s.label_lon != null ? (
                        <Link onClick={() => navigate("label", s.label_id, s.label_lon, s.label_lat)} color={colors.label}>
                            {s.label}
                        </Link>
                    ) : s.label}
                </div>
            )}
            <div className="flex items-center gap-2 mt-2 text-xs text-muted">
                {s.release_date && <span>{s.release_date.slice(0, 4)}</span>}
                {s.track_popularity != null && (
                    <span className="bg-muted/10 px-1.5 py-0.5 rounded">pop {s.track_popularity}</span>
                )}
            </div>
        </>
    );
}

function AlbumPanel({ s, navigate }) {
    return (
        <>
            <div className="text-lg font-semibold leading-snug truncate">{s.album_name}</div>
            {s.artist_name && (
                <div className="italic font-medium leading-tight text-sm mt-0.5 truncate">
                    {s.artist_lon != null ? (
                        <Link onClick={() => navigate("artist", s.artist_rowid, s.artist_lon, s.artist_lat)} color={colors.artist}>
                            {s.artist_name}
                        </Link>
                    ) : s.artist_name}
                </div>
            )}
            {s.label && (
                <div className="text-xs text-muted mt-0.5 truncate">
                    {s.label_lon != null ? (
                        <Link onClick={() => navigate("label", s.label_id, s.label_lon, s.label_lat)} color={colors.label}>
                            {s.label}
                        </Link>
                    ) : s.label}
                </div>
            )}
            <div className="flex items-center gap-2 mt-2 text-xs text-muted">
                {s.release_date && <span>{s.release_date.slice(0, 4)}</span>}
                {s.popularity != null && (
                    <span className="bg-muted/10 px-1.5 py-0.5 rounded">pop {s.popularity}</span>
                )}
            </div>
        </>
    );
}

function ArtistPanel({ s }) {
    return (
        <>
            <div className="text-lg font-semibold leading-snug truncate">{s.artist_name}</div>
            {s.genre && <div className="text-xs text-muted truncate">{s.genre}</div>}
            {s.track_count != null && (
                <div className="text-xs text-muted mt-2 truncate">{s.track_count} tracks</div>
            )}
        </>
    );
}

function LabelPanel({ s }) {
    return (
        <>
            <div className="text-lg font-semibold leading-snug truncate">{s.label}</div>
            {s.track_count != null && (
                <div className="text-xs text-muted mt-2 truncate">{s.track_count} tracks</div>
            )}
        </>
    );
}

export default function Panel({ selection, navigate, onClose }) {
    const [recsOpen, setRecsOpen] = useState(false);
    const [recs, setRecs]         = useState(null);
    const [rolling, setRolling]   = useState(false);

    useEffect(() => {
        setRecsOpen(false);
        setRecs(null);
    }, [selection]);

    if (!selection) return null;

    const { entityType } = selection;

    let body;
    if (selection.loading) {
        body = <LoadingBody />;
    } else if (selection.error) {
        body = <div className="text-muted text-sm">Failed to load.</div>;
    } else if (entityType === "track") {
        body = <TrackPanel s={selection} navigate={navigate} />;
    } else if (entityType === "album") {
        body = <AlbumPanel s={selection} navigate={navigate} />;
    } else if (entityType === "artist") {
        body = <ArtistPanel s={selection} />;
    } else if (entityType === "label") {
        body = <LabelPanel s={selection} />;
    }

    const hasData = !selection.loading && !selection.error;

    const handleRecroll = () => {
        const rowid = getRowid(entityType, selection);
        setRolling(true);
        fetch(`/api/recroll?q=${rowid}&entity=${entityType}`)
            .then(r => r.json())
            .then(data => {
                setRolling(false);
                navigate(entityType, getRowid(entityType, data), data.lon, data.lat);
            })
            .catch(() => setRolling(false));
    };

    const handleToggleRecs = () => {
        if (recsOpen) { setRecsOpen(false); return; }
        setRecsOpen(true);
        if (recs !== null) return;
        const rowid = getRowid(entityType, selection);
        setRecs({ loading: true });
        const diverse = entityType === "track" || entityType === "album";
        fetch(`/api/recs?q=${rowid}&entity=${entityType}&diverse=${diverse}`)
            .then(r => r.json())
            .then(data => setRecs(data))
            .catch(() => setRecs({ error: true }));
    };

    return (
        <div
            className="fixed z-10 bottom-0 left-0 right-0
                       sm:bottom-4 sm:left-3 sm:top-auto sm:right-auto sm:w-md
                       max-h-[60dvh] sm:max-h-[calc(100vh-6rem)]
                       overflow-y-auto overscroll-contain
                       bg-surface font-sans text-base p-5
                       rounded-t-2xl sm:rounded-xl shadow-xl
                       touch-none sm:touch-auto"
            style={{ paddingBottom: "calc(1rem + env(safe-area-inset-bottom))" }}
            onPointerDown={e => e.stopPropagation()}
            onPointerMove={e => e.stopPropagation()}
        >
            <div className="flex items-start gap-3">
                <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2 mb-2">
                        <Badge entityType={entityType} />
                    </div>
                    {body}
                </div>
                <div className="flex flex-col items-end gap-2 flex-shrink-0">
                    <button
                        onClick={onClose}
                        className="text-muted hover:text-white transition-colors text-lg leading-none"
                        aria-label="Close"
                    >×</button>
                    {hasData && (
                        <button
                            onClick={handleRecroll}
                            disabled={rolling}
                            style={{ backgroundColor: rolling ? undefined : "#3bda28", color: "#000" }}
                            className="font-bold text-base px-4 py-3 rounded-xl
                                       disabled:opacity-40 disabled:bg-muted/20 disabled:text-muted"
                        >
                            {rolling ? "…" : "Find more"}
                        </button>
                    )}
                    {selection.loading && (
                        <div className="h-12 w-20 bg-muted/20 rounded-xl animate-pulse" />
                    )}
                </div>
            </div>
            {hasData && (
                <div className="mt-3 border-t border-muted/20 pt-2">
                    <button
                        onClick={handleToggleRecs}
                        className="text-muted text-xs flex items-center gap-1 hover:text-white transition-colors"
                    >
                        Similiar {recsOpen ? "▲" : "▼"}
                    </button>
                    {recsOpen && (
                        <RecsList recs={recs} entityType={entityType} navigate={navigate} />
                    )}
                </div>
            )}
        </div>
    );
}
