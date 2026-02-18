# spotify-rec

## Setup
```bash
uv sync                  # default: PyTorch with latest CUDA
uv sync --extra cu118    # PyTorch with CUDA 11.8 (older GPUs, e.g. RTX 2060)
```

## Cloud Computing
From project root, to vm:
```bash
rsync -avz --exclude '.idea' --exclude 'data/raw'  --exclude 'data/clean' --exclude '.venv' --exclude '__pycache__' ./ ubuntu@150.136.147.169:~/spotify-rec/
```

to local machine:
```bash
rsync -avz --exclude '.idea' --exclude 'data/raw' --exclude 'data/clean' --exclude '.venv' --exclude '__pycache__' ubuntu@150.136.147.169:~/spotify-rec/ ./
```

## Data Pipeline

### 1. Merge databases
```bash
python scripts/merge_databases.py \
    --spotify <spotify_clean.sqlite3> \
    --audio <audio_features.sqlite3> \
    <output_merged.sqlite3>
```

### 2. Extract metadata
```bash
python scripts/extract_metadata.py <merged.sqlite3> -p 50 -g
```
- `-p`: popularity threshold (default: 80)
- `-g`: include artist genres

Output: `data/raw/training_pop{POP}[_genres].parquet`

### 3. Clean metadata
```bash
python scripts/clean_metadata.py <raw_dataset.parquet>
```
Input defaults to `data/raw/` if filename only.
Output: `data/clean/<filename>.parquet`

### 4. Run feature selection
```bash
python scripts/engineer_features <clean_dataset.parquet>
```

Input defaults to `data/clean/` if filename only.
Output: `data/engineered/<filename>.parquet`

