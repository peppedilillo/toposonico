## Setup

```sh
uv sync
```

Torch and cuML are not bundled as optional dependencies — install the right wheels for your
CUDA version manually.

CUDA 12.x:
```sh
uv pip install torch torchvision --index-url https://download.pytorch.org/whl/cu128
uv pip install cuml-cu12 --extra-index-url https://pypi.nvidia.com/cuml-cu12/
```

CUDA 11.8:
```sh
uv pip install torch torchvision --index-url https://download.pytorch.org/whl/cu118
uv pip install cuml-cu11 --extra-index-url https://pypi.nvidia.com/cuml-cu11/
```

## Pipeline

### Phase 0 - Configuration

```sh
cp config.sample.env config.env
# vim config.env  # compile it
source config.env
```

Now the scripts should be wired to run with default inputs and output in `/outs` directory.


### Phase 1 — Data prep (one-time; slow — full DB scan)

```sh
python scripts/build_training_vocab.py --min-count 2
python scripts/build_playlist_chunks.py path/to/chunks
python scripts/build_track_lookup.py
python scripts/build_entity_lookups.py
```

We will use `build_track_lookup.py` again so take note of where it lives.
There is a lot of data, choose a directory for chunks wisely

### Phase 2 — Training (interactive, GPU)

> Use the `lambda_sniper.py` script for catching lambdalabs instances!

```sh
# open notebooks/train.ipynb, do your thing and store the checkpoint to `path/to/<checkpoint>.pt`
python scripts/build_embeddings.py path/to/<checkpoint>.pt
```

This script will also perform clean-up of tracks with duplicated ISRC.
You can skip deduplication passing `--no-filter`.

### Phase 3 — UMAP projection (interactive, GPU)

```sh
# open notebooks/umap.ipynb, do your thing and store them somewhere safe
```

Voilà! Take note of the umap location for all entities, we will use it in the next phases.

> **Warning:** all four entity UMAPs must come from the same UMAP fit — tracks are
> `fit_transform`ed first, and albums/artists/labels are `transform`ed through that same
> fitted model. Mixing parquets from different fits produces incompatible coordinates and
> silently corrupts the geo map.

