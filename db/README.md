## Setup

Install with:

```sh
uv sync
```

Or equivalent.

## Usage

```sh
cp config.sample.env config.env
# vim config.env  # set SICK_MANIFEST and SICK_DB at minimum
source config.env
bash build.sh
```

Should be it. Now run:

```shell
uv run scripts/manifest.py > manifest.toml
```

Fill it with the absolute paths to DB output, faiss indexes and geojson path.
The geo parquet are to be considered temporary artifacts, you can delete them once you are done, if you want.
