import {useEffect, useState} from 'react';

export default function Search({mapRef}) {
    const [query, setQuery] = useState('');
    const [results, setResults] = useState([]);

    useEffect(() => {
        if (!query.trim()) {
            setResults([]);
            return;
        }
        const timer = setTimeout(() => {
            fetch(`/api/search?q=${encodeURIComponent(query)}`)
                .then(r => r.json())
                .then(hits => setResults(hits))
                .then(console.log(results));
        }, 300);

        return () => clearTimeout(timer)
    }, [query]);

    return (
        <div style={{position: 'absolute', top: 16, left: 16, zIndex: 10}}>
            <input
                value={query}
                onChange={e => setQuery(e.target.value)}
                placeholder="Search…"
            />
            {results.length > 0 && (
                <ul style={{margin: 0, padding: 0, listStyle: 'none'}}>
                    {results.map(hit => (
                        <li key={hit.id} onClick={() => {
                            mapRef.current.flyTo({center: [hit.lon, hit.lat], zoom: 8});
                            setQuery('');
                            setResults([]);
                        }}>
                            {hit.entity_type} — {hit.track_name ?? hit.album_name ?? hit.artist_name
                            ?? hit.label}
                        </li>
                    ))}
                </ul>
            )}
        </div>
    );
}