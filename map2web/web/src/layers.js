import colors from "./theme.js";

/**
 * @typedef {Object} LayerDef
 * @property {string} id            MapLibre layer id
 * @property {string} sourceLayer   vector tile source-layer name
 * @property {string} entityType    'track' | 'album' | 'artist' | 'label'
 * @property {string} rowidProp     tile feature property that holds the entity rowid
 * @property {string} char          symbol character rendered on the map
 * @property {number} size          text-size in pixels
 * @property {string} color         CSS color string
 * @property {number} opacity       text-opacity [0, 1]
 */
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

const MAP_ET2LAYER = Object.fromEntries(LAYERS.map((l) => [l.entityType, l]));

export { LAYERS, MAP_ET2LAYER };
