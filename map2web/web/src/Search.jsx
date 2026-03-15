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
function hitlabel(hit) {
    if (hit.entity_type === "track")
        return `${hit.artist_name} - ${hit.track_name}`;
    if (hit.entity_type === "album")
        return `${hit.artist_name} - ${hit.album_name}`;
    if (hit.entity_type === "artist") return `${hit.artist_name}`;
    if (hit.entity_type === "label") return `${hit.label}`;
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
        <div className="absolute top-3 left-3 z-10 font-mono text-base">
            <input
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="search…"
                className="bg-transparent border-0 outline-none"
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
                <ul className="m-0 p-0 list-none">
                    {results.map((hit, i) => (
                        <li
                            key={hit.id}
                            onClick={() => fly(hit)}
                            className={i === activeIdx
                                ? "cursor-pointer py-0.5 px-1 text-foreground bg-overlay"
                                : "cursor-pointer py-0.5 px-1 text-muted"}
                            onMouseEnter={() => setActiveIdx(i)}
                            onMouseLeave={() => setActiveIdx(null)}
                        >
                            [{MAP_ET2LAYER[hit.entity_type].char}] {hitlabel(hit)}
                        </li>
                    ))}
                </ul>
            )}
        </div>
    );
}
