"""
Prints a TOML data manifest template.

Usage:
    python scripts/manifest.py > manifest.toml
"""

import argparse
from datetime import datetime

MANIFEST = (
    """#                                               SICK MANIFEST
"""
    f"#                                        {datetime.now().isoformat()}"
    """
# use absolute paths and keep the quotes. track_db refers to the `track_clean.db` SQlite db used as source.
sick_db = ""
faiss_track = ""
faiss_album = ""
faiss_artist = ""
faiss_label = ""
"""
)


def main():
    parser = argparse.ArgumentParser(
        description="Prints a manifest template. Usage: 'python scripts/manifest.py > manifest.toml'",
    )
    _ = parser.parse_args()
    print(MANIFEST, end="")


if __name__ == "__main__":
    main()
