# annas-umap

Projects track embeddings into 2D via UMAP (cuML GPU implementation).

## Setup

```bash
uv sync # base deps (numpy, pandas, pyarrow, jupyterlab)
uv pip install cuml-cu12 --extra-index-url https://pypi.nvidia.com/cuml-cu12/
# or if cuda11 is required:
# uv pip install cuml-cu11 --extra-index-url https://pypi.nvidia.com/cuml-cu11/
```

## Usage

1. Export embeddings from a trained w2v checkpoint:
   `w2v/scripts/export_embeddings.py`
2. Set UMAP params in cell 2 of `project.ipynb` (`n_neighbors`, `min_dist`)
3. Run `project.ipynb` on the cloud machine
