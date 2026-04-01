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
[source]
track_db = ""

[embedding]
track  = ""
artist = ""
album  = ""
label  = ""

[lookup]
track  = ""
artist = ""
album  = ""
label  = ""

[umap]
track  = ""
artist = ""
album  = ""
label  = ""
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
