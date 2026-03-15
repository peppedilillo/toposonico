import colors from "./theme.js";

const LAYERS = [
    {
        id: "tracks",
        sourceLayer: "tracks",
        char: "·",
        size: 16,
        color: colors.track,
        opacity: 0.7,
        info: (p) => ({
            entityType: "track",
            line1: p.track_name,
            line2: p.artist_name,
        }),
    },
    {
        id: "artists",
        sourceLayer: "artists",
        char: "*",
        size: 12,
        color: colors.artist,
        opacity: 1.0,
        info: (p) => ({
            entityType: "artist",
            line2: p.artist_name,
        }),
    },
    {
        id: "albums",
        sourceLayer: "albums",
        char: "o",
        size: 12,
        color: colors.album,
        opacity: 0.7,
        info: (p) => ({
            entityType: "album",
            line1: p.album_name,
            line2: p.artist_name,
        }),
    },
    {
        id: "labels",
        sourceLayer: "labels",
        char: "P",
        size: 24,
        color: colors.label,
        opacity: 1.0,
        info: (p) => ({
            entityType: "label",
            line1: p.label,
        }),
    },
];

const MAP_ET2LAYER = {
    track: LAYERS[0],
    artist: LAYERS[1],
    album: LAYERS[2],
    label: LAYERS[3],
};

export { LAYERS, MAP_ET2LAYER };
