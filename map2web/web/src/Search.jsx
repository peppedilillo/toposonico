import { useEffect, useState } from "react";
import { MAP_ET2LAYER } from "./layers.js";

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
    const line1 = type === "track" || type === "album" ? hit.artist_name : null;
    const line2 = type === "track" ? hit.track_name
        : type === "album" ? hit.album_name
        : type === "label" ? hit.label
        : hit.artist_name;
    return <>
        {line1 && <div className="italic font-medium leading-tight text-sm truncate">{line1}</div>}
        <div className="font-semibold truncate">{line2}</div>
        <div className="text-muted text-xs mt-0.5">
            <span className="uppercase tracking-wider text-[9px] bg-muted/10 px-1.5 py-0.5 rounded">
                {type}
            </span>
        </div>
    </>;
}

export default function Search({ mapRef, setSelection, results, setResults }) {
    const [query, setQuery] = useState("");
    const [activeIdx, setActiveIdx] = useState(null);

    useEffect(() => {
        if (!query.trim()) {
            setResults([]);
            setActiveIdx(null);
            return;
        }
        const timer = setTimeout(() => {
            fetch(`/api/search?q=${encodeURIComponent(query)}`)
                .then((r) => r.json())
                .then((hits) => {
                    setResults(hits);
                    setActiveIdx(null);
                })
                .catch(() => setResults([]));
        }, 300);

        return () => clearTimeout(timer);
    }, [query]);

    /** @param {Hit} hit */
    const fly = (hit) => {
        const map = mapRef.current;
        const layer = MAP_ET2LAYER[hit.entity_type];
        map.flyTo({ center: [hit.lon, hit.lat], zoom: 11 });
        map.once("moveend", () => setSelection(layer.info(hit)));
        setQuery("");
        setResults([]);
    };

    return (
        <div
            className="absolute top-3 z-100 font-sans text-base left-1/2 -translate-x-1/2 w-11/12 sm:left-3 sm:translate-x-0 sm:w-md touch-none sm:touch-auto"
            onPointerDown={e => e.stopPropagation()}
            onPointerMove={e => e.stopPropagation()}
        >
            <input
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="search…"
                className="w-full bg-surface text-white placeholder:text-muted p-2 pl-5 rounded-xl"
                onKeyDown={(e) => {
                    if (e.key === "ArrowDown")
                        setActiveIdx((i) =>
                            i === null
                                ? 0
                                : Math.min(i + 1, results.length - 1),
                        );
                    else if (e.key === "ArrowUp")
                        setActiveIdx((i) =>
                            i === null
                                ? results.length - 1
                                : Math.max(i - 1, 0),
                        );
                    else if (e.key === "Enter" && activeIdx !== null)
                        fly(results[activeIdx]);
                    else if (e.key === "Escape") {
                        setQuery("");
                        setResults([]);
                        setActiveIdx(null);
                    }
                }}
            />
            {results.length > 0 && (
                <ul className="bg-surface mt-1 py-2 list-none rounded-xl">
                    {results.slice(0, 3).map((hit, i) => (
                        <li
                            key={hit.id}
                            onClick={() => fly(hit)}
                            className={i === activeIdx
                                ? "cursor-pointer px-5 py-2 bg-overlay"
                                : "cursor-pointer px-5 py-2 "}
                            onMouseEnter={() => setActiveIdx(i)}
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
