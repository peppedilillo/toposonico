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