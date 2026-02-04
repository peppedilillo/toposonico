# spotify-rec

## Cloud Computing
From project root

```bash
rsync -avz --exclude '.venv' --exclude '__pycache__' ./ ubuntu@<LAMBDA_IP>:~/spotify-rec/
```


## Data Pipeline

### 1. Merge databases
```bash
python scripts/merge_databases.py \
    --spotify <spotify_clean.sqlite3> \
    --audio <audio_features.sqlite3> \
    <output_merged.sqlite3>
```

### 2. Extract training data
```bash
python scripts/extract_training_data.py <merged.sqlite3> -p 50 -g
```
- `-p`: popularity threshold (default: 80)
- `-g`: include artist genres

Output: `data/raw/training_pop{POP}[_genres].parquet`

### 3. Clean data
```bash
python scripts/clean_data.py <raw_dataset.parquet>
```
Input defaults to `data/raw/` if filename only.
Output: `data/clean/<filename>.parquet`

### 4. Run feature selection
```bash
python scripts/engineer_features <clean_dataset.parquet>
```

Input defaults to `data/clean/` if filename only.
Output: `data/engineered/<filename>.parquet`

