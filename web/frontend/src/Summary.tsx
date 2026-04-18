/** Maps entity type to its theme color CSS variable value. */
const ENTITY_COLORS: Record<string, string> = {
  track: "var(--color-track)",
  album: "var(--color-album)",
  artist: "var(--color-artist)",
  label: "var(--color-label)",
};

type BadgeProps = { entityType: "track" | "album" | "artist" | "label" };

/** Colored pill displaying the entity type. */
function Badge({ entityType }: BadgeProps) {
  return (
    <span
      className="uppercase tracking-wider text-[10px] px-1.5 py-0.5 rounded font-semibold"
      style={{
        color: ENTITY_COLORS[entityType],
        background: "rgba(255,255,255,0.07)",
      }}
    >
      {entityType}
    </span>
  );
}

type TrackSummaryProps = {
  track: React.ReactNode;
  artist: React.ReactNode;
  album?: React.ReactNode;
};

type AlbumSummaryProps = {
  albumName: React.ReactNode;
  artist: React.ReactNode;
};

type ArtistSummaryProps = {
  artistName: React.ReactNode;
  genre?: React.ReactNode;
  playlistCount: React.ReactNode;
};

type LabelSummaryProps = {
  labelName: React.ReactNode;
  playlistCount: React.ReactNode;
};

/** Summary header for track entities. */
export function TrackSummary({
  track,
  artist,
  album,
}: TrackSummaryProps) {
  return (
    <div className="space-y-1.5">
      <Badge entityType="track" />
      <div className="text-lg font-semibold leading-snug text-white truncate">
        {track}
      </div>
      <div className="text-sm text-muted leading-tight truncate">
        {artist}
        {album ? <> · {album}</> : null}
      </div>
    </div>
  );
}

/** Summary header for album entities. */
export function AlbumSummary({
  albumName,
  artist,
}: AlbumSummaryProps) {
  return (
    <div className="space-y-1.5">
      <Badge entityType="album" />
      <div className="text-lg font-semibold leading-snug text-white truncate">
        {albumName}
      </div>
      <div className="text-sm text-muted leading-tight truncate">{artist}</div>
    </div>
  );
}

/** Summary header for artist entities. */
export function ArtistSummary({
  artistName,
  genre,
  playlistCount,
}: ArtistSummaryProps) {
  return (
    <div className="space-y-1.5">
      <Badge entityType="artist" />
      <div className="text-lg font-semibold leading-snug text-white truncate">
        {artistName}
      </div>
      <div className="text-sm text-muted leading-tight truncate">
        {genre ? (
          <>
            {genre} · {playlistCount}
          </>
        ) : (
          playlistCount
        )}
      </div>
    </div>
  );
}

/** Summary header for label entities. */
export function LabelSummary({
  labelName,
  playlistCount,
}: LabelSummaryProps) {
  return (
    <div className="space-y-1.5">
      <Badge entityType="label" />
      <div className="text-lg font-semibold leading-snug text-white truncate">
        {labelName}
      </div>
      <div className="text-sm text-muted leading-tight truncate">
        {playlistCount}
      </div>
    </div>
  );
}
