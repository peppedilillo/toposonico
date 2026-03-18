import {Badge} from "./Badge.jsx";


export function Link({onClick, children}) {
    return (
        <button
            onClick={onClick}
            className="cursor-pointer text-left underline underline-offset-2 hover:opacity-70 transition-opacity"
        >
            {children}
        </button>
    );
}

function TrackPanel({ s, navigate }) {
    return (
        <>
            <div className="text-lg font-semibold leading-snug">
                {s.track_name}
            </div>
            <div className="italic font-medium leading-tight text-sm mt-0.5">
                {s.artist_lon != null ? (
                    <Link
                        onClick={() =>
                            navigate(
                                "artist",
                                s.artist_rowid,
                                s.artist_lon,
                                s.artist_lat,
                            )
                        }
                    >
                        {s.artist_name}
                    </Link>
                ) : (
                    s.artist_name
                )}
            </div>
            {s.album_name && (
                <div className="text-sm mt-1">
                    {s.album_lon != null ? (
                        <Link
                            onClick={() =>
                                navigate(
                                    "album",
                                    s.album_rowid,
                                    s.album_lon,
                                    s.album_lat,
                                )
                            }
                        >
                            {s.album_name}
                        </Link>
                    ) : (
                        s.album_name
                    )}
                </div>
            )}
            {s.label && (
                <div className="text-xs text-muted mt-0.5">
                    {s.label_lon != null ? (
                        <Link
                            onClick={() =>
                                navigate(
                                    "label",
                                    s.label_id,
                                    s.label_lon,
                                    s.label_lat,
                                )
                            }
                        >
                            {s.label}
                        </Link>
                    ) : (
                        s.label
                    )}
                </div>
            )}
            <div className="flex items-center gap-2 mt-2 text-xs text-muted">
                {s.release_date && <span>{s.release_date.slice(0, 4)}</span>}
                {s.track_popularity != null && (
                    <span className="bg-muted/10 px-1.5 py-0.5 rounded">
                        pop {s.track_popularity}
                    </span>
                )}
            </div>
        </>
    );
}

function AlbumPanel({ s, navigate }) {
    return (
        <>
            <div className="text-lg font-semibold leading-snug">
                {s.album_name}
            </div>
            {s.artist_name && (
                <div className="italic font-medium leading-tight text-sm mt-0.5">
                    {s.artist_lon != null ? (
                        <Link
                            onClick={() =>
                                navigate(
                                    "artist",
                                    s.artist_rowid,
                                    s.artist_lon,
                                    s.artist_lat,
                                )
                            }
                        >
                            {s.artist_name}
                        </Link>
                    ) : (
                        s.artist_name
                    )}
                </div>
            )}
            {s.track_count != null && (
                <div className="text-xs text-muted mt-2">
                    {s.track_count} tracks
                </div>
            )}
        </>
    );
}

function ArtistPanel({ s }) {
    return (
        <>
            <div className="text-lg font-semibold leading-snug">
                {s.artist_name}
            </div>
            {s.track_count != null && (
                <div className="text-xs text-muted mt-2">
                    {s.track_count} tracks
                </div>
            )}
        </>
    );
}

function LabelPanel({ s }) {
    return (
        <>
            <div className="text-lg font-semibold leading-snug">{s.label}</div>
            {s.track_count != null && (
                <div className="text-xs text-muted mt-2">
                    {s.track_count} tracks
                </div>
            )}
        </>
    );
}

export default function Panel({ selection, navigate, onClose }) {
    if (!selection) return null;

    const { entityType } = selection;

    let body;
    if (selection.loading) {
        body = <div className="text-muted text-sm animate-pulse">Loading…</div>;
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

    return (
        <div
            className="fixed z-10 bottom-0 left-0 right-0
                       sm:bottom-4 sm:left-3 sm:top-auto sm:right-auto sm:w-md
                       max-h-[60dvh] sm:max-h-[calc(100vh-6rem)]
                       overflow-y-auto overscroll-contain
                       bg-surface font-sans text-base p-5
                       rounded-t-2xl sm:rounded-xl shadow-xl
                       touch-none sm:touch-auto"
            style={{
                paddingBottom: "calc(1rem + env(safe-area-inset-bottom))",
            }}
            onPointerDown={(e) => e.stopPropagation()}
            onPointerMove={(e) => e.stopPropagation()}
        >
            <div className="flex items-start justify-between gap-2 mb-2">
                <Badge entityType={entityType} />
                <button
                    onClick={onClose}
                    className="text-muted hover:text-white transition-colors text-lg leading-none"
                    aria-label="Close"
                >
                    ×
                </button>
            </div>
            {body}
        </div>
    );
}
