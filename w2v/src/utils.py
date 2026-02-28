import pandas as pd
import torch



def print_vram_budget(vocab_size: int, batch_size: int, embedding_dims: int, neg_samples: int, neg_samples_block: int):
    """ Prints VRAM usage summary.
    """
    # it seems that nvidia-smi does not use MiB/GiB despite printing them for unit of measure?
    MB = 1 / 1_000_000
    GB = 1 / 1_000_000_000
    BYTES_IN_32BIT = 4
    BYTES_IN_64BIT = 8

    embed    = 2 * vocab_size * embedding_dims * BYTES_IN_32BIT
    optim    = 2 * embed
    weights_ = vocab_size * BYTES_IN_32BIT
    negs     = batch_size * neg_samples * neg_samples_block * BYTES_IN_64BIT # torch.long
    act_fwd  = (2 + neg_samples) * batch_size * embedding_dims * BYTES_IN_32BIT # the ` 2 + ..` is for the centers and context
    act_bwd  = act_fwd

    total    = embed + optim + weights_ + act_fwd + act_bwd

    print(f"Vocab size          : {vocab_size:>10,}")
    print()
    print(f"Embedding tables    : {embed    * GB:>6.2f} GB  (2 tables × {vocab_size:,} × {embedding_dims} × fp32)")
    print(f"Optimizer state     : {optim    * GB:>6.2f} GB  (SparseAdam exp_avg + exp_avg_sq, dense after warmup)")
    print(f"Weights             : {weights_ * MB:>6.1f} MB  (negative sample weights)")
    print(f"Neg. sample block   : {negs     * MB:>6.1f} MB  (negative sampling reservoir)")
    print(f"Activations fwd     : {act_fwd  * MB:>6.1f} MB  ({2 + neg_samples} tensors × {batch_size:,} × {embedding_dims})")
    print(f"Activations bwd     : {act_bwd  * MB:>6.1f} MB  (sparse grad upper bound)")
    print()
    print(f"Total estimate      : {total    * GB:>6.2f} GB")
    if torch.cuda.is_available():
        vram_total = torch.cuda.get_device_properties(0).total_memory
        vram_free, _ = torch.cuda.mem_get_info()
        headroom = vram_free - total
        flag = "OK" if headroom > 0 else "OOM RISK"
        print()
        print(f"GPU                 : {torch.cuda.get_device_properties(0).name}")
        print(f"VRAM total          : {vram_total * GB:>6.2f} GB")
        print(f"VRAM free now       : {vram_free  * GB:>6.2f} GB")
        print(f"VRAM used now       : {(vram_total - vram_free)  * GB:>6.2f} GB")
        print(f"Headroom            : {headroom   * GB:>+6.2f} GB  [{flag}]")


def print_ram_budget(vocab_size: int, embedding_dims: int):
    """Prints CPU RAM usage estimates for loading the checkpoint and running inference.

    Two phases are costed separately:
      - Checkpoint load  : torch.load materialises both embedding tables into RAM.
      - Inference peak   : model kept in RAM + a normalised copy of embeddings_in
                           for cosine similarity (emb / emb.norm(...)).
    """
    GB = 1 / 1_000_000_000
    BYTES_IN_32BIT = 4

    checkpoint = 2 * vocab_size * embedding_dims * BYTES_IN_32BIT
    inference  = 3 * vocab_size * embedding_dims * BYTES_IN_32BIT  # + normalised emb_in

    print(f"Vocab size          : {vocab_size:>10,}")
    print()
    print(f"Checkpoint load     : {checkpoint * GB:>6.2f} GB  (both embedding tables)")
    print(f"Inference peak      : {inference  * GB:>6.2f} GB  (+ normalised embeddings_in)")

    try:
        meminfo = {}
        with open("/proc/meminfo") as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 2:
                    meminfo[parts[0].rstrip(":")] = int(parts[1]) * 1024
        ram_total     = meminfo["MemTotal"]
        ram_available = meminfo["MemAvailable"]
        headroom = ram_available - inference
        flag = "OK" if headroom > 0 else "OOM RISK"
        print()
        print(f"RAM total           : {ram_total     * GB:>6.2f} GB")
        print(f"RAM available now   : {ram_available * GB:>6.2f} GB")
        print(f"Headroom            : {headroom      * GB:>+6.2f} GB  [{flag}]")
    except Exception:
        pass


def print_vocab_stats(vocab: pd.DataFrame):
    """ Prints stats from a track vocabulary."""
    print(f"Track vocab size : {len(vocab):>10,}")
    print(f"Total interactions : {vocab["playlist_count"].sum():>10,}")
    print(f"Track count p50  : {vocab['playlist_count'].median():.0f}")
    print(f"Track count p99  : {vocab['playlist_count'].quantile(0.99):.0f}")
    print(f"Track count min  : {vocab['playlist_count'].min()}")
    print(f"Track count max  : {vocab['playlist_count'].max()}")
