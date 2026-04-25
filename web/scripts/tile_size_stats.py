"""Report size statistics for an MBTiles archive."""

import argparse
import os
from pathlib import Path
import sqlite3


def format_bytes(size: float) -> str:
    units = ("B", "KiB", "MiB", "GiB")
    value = float(size)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f"{value:,.2f} {unit}" if unit != "B" else f"{value:,.0f} {unit}"
        value /= 1024
    raise AssertionError("unreachable")


def main():
    parser = argparse.ArgumentParser(
        description="Report total, mean, max, and count statistics for map tiles.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--tiles",
        default=os.environ.get("SICK_TILES_MB"),
        metavar="PATH",
        help="Path to MBTiles archive. $SICK_TILES_MB",
    )
    args = parser.parse_args()

    if args.tiles is None:
        raise ValueError("--tiles / $SICK_TILES_MB not set")

    tiles = Path(args.tiles)
    if not tiles.is_file():
        raise FileNotFoundError(f"MBTiles file not found: {tiles}")

    conn = sqlite3.connect(tiles)
    count, total_size, mean_size, max_size = conn.execute(
        """
        SELECT
            COUNT(*),
            COALESCE(SUM(LENGTH(tile_data)), 0),
            COALESCE(AVG(LENGTH(tile_data)), 0),
            COALESCE(MAX(LENGTH(tile_data)), 0)
        FROM tiles
        """
    ).fetchone()
    max_tile = conn.execute(
        """
        SELECT
            zoom_level,
            tile_column,
            ((1 << zoom_level) - 1 - tile_row) AS tile_row_xyz
        FROM tiles
        ORDER BY LENGTH(tile_data) DESC, zoom_level DESC, tile_column DESC, tile_row DESC
        LIMIT 1
        """
    ).fetchone()
    conn.close()

    if count == 0:
        mean_size = 0

    print("tile size stats")
    print(tiles)
    print()
    print(f"  tiles       {count:>12,}")
    print(f"  total size  {total_size:>12,}  {format_bytes(total_size)}")
    print(f"  mean size   {mean_size:>12,.2f}  {format_bytes(mean_size)}")
    print(f"  max size    {max_size:>12,}  {format_bytes(max_size)}")
    if max_tile is not None:
        zoom, x, y = max_tile
        print(f"  max tile    {zoom}/{x}/{y}")


if __name__ == "__main__":
    main()
