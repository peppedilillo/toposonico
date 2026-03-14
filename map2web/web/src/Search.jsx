import {useEffect, useState} from 'react';

export default function Search({mapRef}) {
    const [query, setQuery] = useState('');
    const [results, setResults] = useState([]);

    useEffect(() => {
        if (!query.trim()) { setResults([]); return; }
        const timer = setTimeout(() => {
            fetch(`/api/search?q=${encodeURIComponent(query)}`)
                .then(r => r.json())
                .then(hits => setResults(hits));
        }, 300);
        return () => clearTimeout(timer);
    }, [query]);

    const fly = (hit) => {
        mapRef.current.flyTo({center: [hit.lon, hit.lat], zoom: 8});
        setQuery('');
        setResults([]);
    };

    return (
        <div style={{position: 'absolute', top: 12, left: 12, zIndex: 10, fontFamily: 'monospace', fontSize: 12}}>
            <input
                value={query}
                onChange={e => setQuery(e.target.value)}
                placeholder="search…"
                style={{
                    background: 'transparent',
                    font: 'inherit',
                    border: 'none', // Optional: provides a baseline for visibility
                    outline: 'none'      // Optional: cleaner look on focus
                }}
            />
            {results.length > 0 && (
                <ul style={{margin: 0, padding: 0, listStyle: 'none'}}>
                    {results.map(hit => (
                        <li key={hit.id} onClick={() => fly(hit)} style={{cursor: 'pointer'}}>
                            {hit.entity_type} — {hit.track_name ?? hit.album_name ?? hit.artist_name ?? hit.label}
                        </li>
                    ))}
                </ul>
            )}
        </div>
    );
}
