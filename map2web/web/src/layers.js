import colors from "./theme.js";

const LAYERS = [
    {
        id: "tracks",
        sourceLayer: "tracks",
        char: "·",
        size: 16,
        color: colors.track,
        opacity: 0.7,
        tooltip: (p) => ({
            entityType: "track",
            line1: p.artist_name,
            line2: p.track_name,
        }),
    },
    {
        id: "artists",
        sourceLayer: "artists",
        char: "*",
        size: 12,
        color: colors.artist,
        opacity: 1.0,
        tooltip: (p) => ({ entityType: "artist", line2: p.artist_name }),
    },
    {
        id: "albums",
        sourceLayer: "albums",
        char: "o",
        size: 12,
        color: colors.album,
        opacity: 0.7,
        tooltip: (p) => ({
            entityType: "album",
            line1: p.artist_name,
            line2: p.album_name,
        }),
    },
    {
        id: "labels",
        sourceLayer: "labels",
        char: "P",
        size: 24,
        color: colors.label,
        opacity: 1.0,
        tooltip: (p) => ({ entityType: "label", line2: p.label }),
    },
];

const MAP_ET2LAYER = {
    track: LAYERS[0],
    artist: LAYERS[1],
    album: LAYERS[2],
    label: LAYERS[3],
};

export { LAYERS, MAP_ET2LAYER };
