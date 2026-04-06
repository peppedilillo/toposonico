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

The geo parquet are to be considered temporary artifacts, you can delete them once you are done, if you want.
