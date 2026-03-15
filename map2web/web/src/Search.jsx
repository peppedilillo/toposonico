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
    const top = type === "track" || type === "album" ? hit.artist_name : null;
    const name = type === "track" ? hit.track_name
        : type === "album" ? hit.album_name
        : type === "label" ? hit.label
        : hit.artist_name;
    return <>
        <div className="truncate">{name}</div>
        <div className="truncate text-muted text-sm">({type}){top && ` ${top}`}</div>
    </>;
}

export default function Search({ mapRef, setTooltip }) {
    const [query, setQuery] = useState("");
    const [results, setResults] = useState([]);
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
        map.once("moveend", () => {
            const { clientWidth: w, clientHeight: h } = map.getContainer();
            setTooltip({
                x: w / 2,
                y: h / 2,
                ...layer.tooltip(hit),
            });
        });
        setQuery("");
        setResults([]);
    };

    return (
        <div className="absolute top-3 z-10 font-sans text-base left-1/2 -translate-x-1/2 w-11/12 sm:left-3 sm:translate-x-0 sm:w-md">
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
                    {results.slice(0, 5).map((hit, i) => (
                        <li
                            key={hit.id}
                            onClick={() => fly(hit)}
                            className={i === activeIdx
                                ? "cursor-pointer py-0.5 px-5 bg-overlay"
                                : "cursor-pointer py-0.5 px-5"}
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
