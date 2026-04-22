"""Report size statistics for a tile directory."""

import argparse
import os
from pathlib import Path


def format_bytes(size: float) -> str:
    units = ("B", "KiB", "MiB", "GiB")
    value = float(size)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f"{value:,.2f} {unit}" if unit != "B" else f"{value:,.0f} {unit}"
        value /= 1024
    raise AssertionError("unreachable")


def iter_tiles(root: Path):
    for path in root.rglob("*.pbf"):
        if path.is_file():
            yield path


def main():
    parser = argparse.ArgumentParser(
        description="Report total, mean, max, and count statistics for map tiles.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--tiles",
        default=os.environ.get("SICK_TILES_DIR"),
        metavar="PATH",
        help="Path to tiles directory. $SICK_TILES_DIR",
    )
    args = parser.parse_args()

    if args.tiles is None:
        raise ValueError("--tiles / $SICK_TILES_DIR not set")

    tiles = Path(args.tiles)

    count = 0
    total_size = 0
    max_size = 0
    max_path = None

    for path in iter_tiles(tiles):
        size = path.stat().st_size
        count += 1
        total_size += size
        if size > max_size:
            max_size = size
            max_path = path

    mean_size = total_size / count if count > 0 else 0

    print("tile size stats")
    print(tiles)
    print()
    print(f"  tiles       {count:>12,}")
    print(f"  total size  {total_size:>12,}  {format_bytes(total_size)}")
    print(f"  mean size   {mean_size:>12,.2f}  {format_bytes(mean_size)}")
    print(f"  max size    {max_size:>12,}  {format_bytes(max_size)}")
    if max_path is not None:
        print(f"  max tile    {max_path.relative_to(tiles)}")


if __name__ == "__main__":
    main()
