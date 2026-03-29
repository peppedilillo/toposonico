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
python scripts/enrich_training_vocab.py
python scripts/build_playlist_chunks.py path/to/chunks
```

The chunks holds a lot of data. Choose a directory wisely.

### Phase 2 — Training (interactive, GPU)

> Use the `lambda_sniper.py` script for catching lambdalabs instances!

```sh
# open notebooks/train.ipynb, do your thing and store the checkpoint to `path/to/<checkpoint>.pt`
python scripts/build_trained_vocab.py
python scripts/build_track_embeddings.py
```

### Phase 4 - Embeddings, lookups

```shell
python scripts/build_lookups.py `path/to/<checkpoint>.pt`
python scripts/build_embeddings.py `path/to/<checkpoint>.pt`
```

### Phase 4 — UMAP projection (interactive, GPU)

```sh
# open notebooks/umap.ipynb, do your thing and store them somewhere safe
```

> **Warning:** all four entity UMAPs must come from the same UMAP fit — tracks are
> `fit_transform`ed first, and albums/artists/labels are `transform`ed through that same
> fitted model. Mixing parquets from different fits produces incompatible coordinates and
> silently corrupts the geo map.

### Phase 4 - Writing manifest

You should now have four UMAP, four embeddings and four lookup tables, one for each entity type.
Get a manifest template with:

```shell
python scripts/manifest.py > manifest.toml
```

Fill it.

----

Voilà! Wasn't a _sick_ ride? You are done, rejoice!

Joking.. Now we can build the db.
