import colors from "./theme.js";

const LAYERS = [
    {
        id: "tracks",
        sourceLayer: "tracks",
        entityType: "track",
        rowidProp: "track_rowid",
        char: "·",
        size: 16,
        color: colors.track,
        opacity: 0.7,
    },
    {
        id: "artists",
        sourceLayer: "artists",
        entityType: "artist",
        rowidProp: "artist_rowid",
        char: "*",
        size: 12,
        color: colors.artist,
        opacity: 0.7,
    },
    {
        id: "albums",
        sourceLayer: "albums",
        entityType: "album",
        rowidProp: "album_rowid",
        char: "o",
        size: 12,
        color: colors.album,
        opacity: 0.7,
    },
    {
        id: "labels",
        sourceLayer: "labels",
        entityType: "label",
        rowidProp: "label_rowid",
        char: "P",
        size: 24,
        color: colors.label,
        opacity: 1.0,
    },
];

const MAP_ET2LAYER = {
    track: LAYERS[0],
    artist: LAYERS[1],
    album: LAYERS[2],
    label: LAYERS[3],
};

export { LAYERS, MAP_ET2LAYER };
