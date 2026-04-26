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
  onZoomIn?: (() => void) | null;
};

type AlbumSummaryProps = {
  albumName: React.ReactNode;
  artist: React.ReactNode;
  onZoomIn?: (() => void) | null;
};

type ArtistSummaryProps = {
  artistName: React.ReactNode;
  genre?: React.ReactNode;
  playlistCount: React.ReactNode;
  onZoomIn?: (() => void) | null;
};

type LabelSummaryProps = {
  labelName: React.ReactNode;
  playlistCount: React.ReactNode;
  onZoomIn?: (() => void) | null;
};

function ZoomTitle({
  children,
  onZoomIn,
}: {
  children: React.ReactNode;
  onZoomIn?: (() => void) | null;
}) {
  if (!onZoomIn) return <>{children}</>;

  return (
    <span
      role="button"
      tabIndex={0}
      onClick={onZoomIn}
      onKeyDown={(e) => {
        if (e.key !== "Enter" && e.key !== " ") return;
        e.preventDefault();
        onZoomIn();
      }}
      className="cursor-pointer"
    >
      {children}
    </span>
  );
}

/** Summary header for track entities. */
export function TrackSummary({ track, artist, album, onZoomIn }: TrackSummaryProps) {
  return (
    <div className="space-y-1">
      <Badge entityType="track" />
      <div className="text-lg font-semibold leading-snug text-white truncate pt-1">
        <ZoomTitle onZoomIn={onZoomIn}>{track}</ZoomTitle>
      </div>
      <div className="text-sm text-muted leading-tight truncate">
        {artist}
        {album ? <> · {album}</> : null}
      </div>
    </div>
  );
}

/** Summary header for album entities. */
export function AlbumSummary({ albumName, artist, onZoomIn }: AlbumSummaryProps) {
  return (
    <div className="space-y-1">
      <Badge entityType="album" />
      <div className="text-lg font-semibold leading-snug text-white truncate pt-1">
        <ZoomTitle onZoomIn={onZoomIn}>{albumName}</ZoomTitle>
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
  onZoomIn,
}: ArtistSummaryProps) {
  return (
    <div className="space-y-1">
      <Badge entityType="artist" />
      <div className="text-lg font-semibold leading-snug text-white truncate pt-1">
        <ZoomTitle onZoomIn={onZoomIn}>{artistName}</ZoomTitle>
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
export function LabelSummary({ labelName, playlistCount, onZoomIn }: LabelSummaryProps) {
  return (
    <div className="space-y-1">
      <Badge entityType="label" />
      <div className="text-lg font-semibold leading-snug text-white truncate pt-1">
        <ZoomTitle onZoomIn={onZoomIn}>{labelName}</ZoomTitle>
      </div>
      <div className="text-sm text-muted leading-tight truncate">
        {playlistCount}
      </div>
    </div>
  );
}
