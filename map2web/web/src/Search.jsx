import { useEffect, useState } from "react";

/**
 * @typedef {Object} Hit
 * @property {string} id
 * @property {'track'|'album'|'artist'|'label'} entity_type
 * @property {string} [track_name]
 * @property {string} [album_name]
 * @property {string} [artist_name]
 * @property {string} [label]
 * @property {number} lon
 * @property {number} lat
 */
/** @param {Hit} hit */
function HitContent({ hit }) {
    const type = hit.entity_type;
    const line1 = type === "track" ? hit.track_name
        : type === "album" ? hit.album_name
        : type === "label" ? hit.label
        : hit.artist_name;
    const line2 = type === "track" || type === "album" ? hit.artist_name : null;
    return <>
        <div className="font-semibold truncate">{line1}</div>
        {line2 && <div className="italic font-medium leading-tight text-sm truncate">{line2}</div>}
        <div className="text-muted text-xs mt-0.5">
            <span className="uppercase tracking-wider text-[9px] bg-muted/10 px-1.5 py-0.5 rounded">
                {type}
            </span>
        </div>
    </>;
}

const DISPLAY_MAX = 3;

export default function Search({ mapRef, selectEntity, results, setResults }) {
    const [query, setQuery] = useState("");
    const [activeIdx, setActiveIdx] = useState(null);
    const [windowStart, setWindowStart] = useState(0);

    const resetNav = () => { setActiveIdx(null); setWindowStart(0); };

    useEffect(() => {
        if (!query.trim()) {
            setResults([]);
            resetNav();
            return;
        }
        const timer = setTimeout(() => {
            fetch(`/api/search?q=${encodeURIComponent(query)}`)
                .then((r) => r.json())
                .then((hits) => { setResults(hits); resetNav(); })
                .catch(() => setResults([]));
        }, 300);

        return () => clearTimeout(timer);
    }, [query]);

    /** @param {Hit} hit */
    const fly = (hit) => {
        const map = mapRef.current;
        map.flyTo({ center: [hit.lon, hit.lat], zoom: 11, essential: true });
        map.once("moveend", () => selectEntity(hit.entity_type, hit.rowid));
        setQuery("");
        setResults([]);
    };

    return (
        <div
            className="absolute top-3 z-100 font-sans text-base left-1/2 -translate-x-1/2
            w-11/12 sm:left-3 sm:translate-x-0 sm:w-md touch-none sm:touch-auto"
            onPointerDown={e => e.stopPropagation()}
            onPointerMove={e => e.stopPropagation()}
        >
            <input
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="search…"
                className="w-full bg-surface text-white placeholder:text-muted p-2 pl-5 rounded-xl"
                onKeyDown={(e) => {
                    if (e.key === "ArrowDown") {
                        const next = activeIdx === null ? 0 : Math.min(activeIdx + 1, results.length - 1);
                        setActiveIdx(next);
                        if (next >= windowStart + DISPLAY_MAX) setWindowStart(next - (DISPLAY_MAX - 1));
                    } else if (e.key === "ArrowUp") {
                        const prev = activeIdx === null ? results.length - 1 : Math.max(activeIdx - 1, 0);
                        setActiveIdx(prev);
                        if (prev < windowStart) setWindowStart(prev);
                    } else if (e.key === "Enter" && activeIdx !== null)
                        fly(results[activeIdx]);
                    else if (e.key === "Escape") {
                        setQuery("");
                        setResults([]);
                        resetNav();
                    }
                }}
            />
            {results.length > 0 && (
                <ul className="bg-surface mt-1 py-2 list-none rounded-xl">
                    {results.slice(windowStart, windowStart + DISPLAY_MAX).map((hit, i) => (
                        <li
                            key={hit.id}
                            onClick={() => fly(hit)}
                            className={windowStart + i === activeIdx
                                ? "cursor-pointer px-5 py-2 bg-overlay"
                                : "cursor-pointer px-5 py-2"}
                            onMouseEnter={() => setActiveIdx(windowStart + i)}
                            onMouseLeave={() => setActiveIdx(null)}
                        >
                            <HitContent hit={hit} />
                        </li>
                    ))}
                </ul>
            )}
        </div>
    );
}
