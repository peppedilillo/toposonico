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
  trackName: React.ReactNode;
  artist: React.ReactNode;
  album?: React.ReactNode;
  debugId?: number;
};

type AlbumSummaryProps = {
  albumName: React.ReactNode;
  artist: React.ReactNode;
  debugId?: number;
};

type ArtistSummaryProps = {
  artistName: React.ReactNode;
  genre?: React.ReactNode;
  playlistCount: React.ReactNode;
  debugId?: number;
};

type LabelSummaryProps = {
  labelName: React.ReactNode;
  playlistCount: React.ReactNode;
  debugId?: number;
};

/** Renders the optional debug rowid shown in panel summaries. TODO: Remove in production. */
function DebugId({ debugId }: { debugId?: number }) {
  if (debugId === undefined) return null;
  return <div className="shrink-0 text-xs text-muted/50">id:{debugId}</div>;
}

/** Summary header for track entities. */
export function TrackSummary({
  trackName,
  artist,
  album,
  debugId,
}: TrackSummaryProps) {
  return (
    <div className="space-y-1.5">
      <Badge entityType="track" />
      <div className="flex items-baseline gap-2">
        <div className="min-w-0 flex-1 text-lg font-semibold leading-snug text-white truncate">
          {trackName}
        </div>
        <DebugId debugId={debugId} />
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
  debugId,
}: AlbumSummaryProps) {
  return (
    <div className="space-y-1.5">
      <Badge entityType="album" />
      <div className="flex items-baseline gap-2">
        <div className="min-w-0 flex-1 text-lg font-semibold leading-snug text-white truncate">
          {albumName}
        </div>
        <DebugId debugId={debugId} />
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
  debugId,
}: ArtistSummaryProps) {
  return (
    <div className="space-y-1.5">
      <Badge entityType="artist" />
      <div className="flex items-baseline gap-2">
        <div className="min-w-0 flex-1 text-lg font-semibold leading-snug text-white truncate">
          {artistName}
        </div>
        <DebugId debugId={debugId} />
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
  debugId,
}: LabelSummaryProps) {
  return (
    <div className="space-y-1.5">
      <Badge entityType="label" />
      <div className="flex items-baseline gap-2">
        <div className="min-w-0 flex-1 text-lg font-semibold leading-snug text-white truncate">
          {labelName}
        </div>
        <DebugId debugId={debugId} />
      </div>
      <div className="text-sm text-muted leading-tight truncate">
        {playlistCount}
      </div>
    </div>
  );
}
