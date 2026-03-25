import random

import pandas as pd
import torch
import re
from pathlib import Path


def print_vram_budget(
    vocab_size: int,
    batch_size: int,
    embedding_dims: int,
    neg_samples: int,
    neg_samples_block: int,
    allocator_overhead: float = 1.30,
):
    """Prints VRAM usage summary.

    Args:
        allocator_overhead: multiplier for PyTorch's CUDA caching allocator.
            nvidia-smi reports *reserved* memory which is typically 20-40%
            above the sum of live tensor allocations.  Default 1.30 (30%).
    """
    MB = 1 / 1_000_000
    GB = 1 / 1_000_000_000
    BYTES_IN_32BIT = 4
    BYTES_IN_64BIT = 8

    embed = 2 * vocab_size * embedding_dims * BYTES_IN_32BIT
    optim = 2 * embed
    weights_ = vocab_size * BYTES_IN_32BIT
    negs = batch_size * neg_samples * neg_samples_block * BYTES_IN_64BIT  # torch.long
    act_fwd = (
        (2 + neg_samples) * batch_size * embedding_dims * BYTES_IN_32BIT
    )  # the ` 2 + ..` is for the centers and context
    act_bwd = act_fwd

    total = embed + optim + weights_ + negs + act_fwd + act_bwd
    total_real = total * allocator_overhead

    print(f"Vocab size          : {vocab_size:>10,}")
    print()
    print(
        f"Embedding tables    : {embed * GB:>6.2f} GB  (2 tables × {vocab_size:,} × {embedding_dims} × fp32)"
    )
    print(
        f"Optimizer state     : {optim * GB:>6.2f} GB  (SparseAdam exp_avg + exp_avg_sq, dense after warmup)"
    )
    print(f"Weights             : {weights_ * MB:>6.1f} MB  (negative sample weights)")
    print(f"Neg. sample block   : {negs * MB:>6.1f} MB  (negative sampling reservoir)")
    print(
        f"Activations fwd     : {act_fwd * MB:>6.1f} MB  ({2 + neg_samples} tensors × {batch_size:,} × {embedding_dims})"
    )
    print(f"Activations bwd     : {act_bwd * MB:>6.1f} MB  (sparse grad upper bound)")
    print()
    print(f"Tensor estimate     : {total * GB:>6.2f} GB")
    print(
        f"Realistic estimate  : {total_real * GB:>6.2f} GB  (×{allocator_overhead:.2f} allocator overhead)"
    )
    if torch.cuda.is_available():
        vram_total = torch.cuda.get_device_properties(0).total_memory
        vram_free, _ = torch.cuda.mem_get_info()
        headroom = vram_free - total_real
        flag = "OK" if headroom > 0 else "OOM RISK"
        print()
        print(f"GPU                 : {torch.cuda.get_device_properties(0).name}")
        print(f"VRAM total          : {vram_total * GB:>6.2f} GB")
        print(f"VRAM free now       : {vram_free * GB:>6.2f} GB")
        print(f"VRAM used now       : {(vram_total - vram_free) * GB:>6.2f} GB")
        print(f"Headroom            : {headroom * GB:>+6.2f} GB  [{flag}]")


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
    inference = 3 * vocab_size * embedding_dims * BYTES_IN_32BIT  # + normalised emb_in

    print(f"Vocab size          : {vocab_size:>10,}")
    print()
    print(f"Checkpoint load     : {checkpoint * GB:>6.2f} GB  (both embedding tables)")
    print(
        f"Inference peak      : {inference * GB:>6.2f} GB  (+ normalised embeddings_in)"
    )

    try:
        meminfo = {}
        with open("/proc/meminfo") as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 2:
                    meminfo[parts[0].rstrip(":")] = int(parts[1]) * 1024
        ram_total = meminfo["MemTotal"]
        ram_available = meminfo["MemAvailable"]
        headroom = ram_available - inference
        flag = "OK" if headroom > 0 else "OOM RISK"
        print()
        print(f"RAM total           : {ram_total * GB:>6.2f} GB")
        print(f"RAM available now   : {ram_available * GB:>6.2f} GB")
        print(f"Headroom            : {headroom * GB:>+6.2f} GB  [{flag}]")
    except Exception:
        pass


def print_vocab_stats(vocab: pd.DataFrame):
    """Prints stats from a track vocabulary."""
    print(f"Track vocab size : {len(vocab):>10,}")
    print(f"Total interactions : {vocab["playlist_count"].sum():>10,}")
    print(f"Track count p50  : {vocab['playlist_count'].median():.0f}")
    print(f"Track count p99  : {vocab['playlist_count'].quantile(0.99):.0f}")
    print(f"Track count min  : {vocab['playlist_count'].min()}")
    print(f"Track count max  : {vocab['playlist_count'].max()}")


_RUN_NAME_RE = re.compile(r"^model_([a-z]+_[a-z]+)_t\w+_ep\d+_v\w+\.pt$")


def extract_run_name(filename: str | Path) -> str:
    """Extracts human hash from standard model filename."""
    name = Path(filename).name
    m = _RUN_NAME_RE.match(name)
    if m is None:
        raise ValueError(
            f"Invalid model filename: {filename!r}. "
            "Expected 'model_<word>_<word>_t<size>_ep<N>_v<version>.pt'"
        )
    return m.group(1)


def make_model_filename(
    run_name: str, vocab_size: int, epoch: int, val_loss: float
) -> str:
    """Parse standard model filename."""

    def format_number(n):
        return next(
            (
                f"{n / d:.0f}" + s
                for d, s in [(1e9, "B"), (1e6, "M"), (1e3, "K")]
                if abs(n) >= d
            ),
            str(n),
        )

    v = f"{val_loss:.4f}".replace(".", "d")
    return f"{run_name}_model_t{format_number(vocab_size)}_ep{epoch}_v{v}.pt"


def human_hash(sep="_"):
    adjectives = [
        "angry",
        "bold",
        "brave",
        "bright",
        "broad",
        "calm",
        "chief",
        "clean",
        "clever",
        "cold",
        "cool",
        "cosmic",
        "cozy",
        "crisp",
        "curious",
        "daring",
        "dark",
        "deep",
        "dizzy",
        "dry",
        "dusty",
        "eager",
        "early",
        "easy",
        "epic",
        "even",
        "fair",
        "fancy",
        "fast",
        "fierce",
        "fine",
        "firm",
        "flat",
        "fond",
        "free",
        "fresh",
        "frozen",
        "funny",
        "fuzzy",
        "gentle",
        "glad",
        "golden",
        "grand",
        "great",
        "green",
        "happy",
        "harsh",
        "hidden",
        "hollow",
        "humble",
        "hungry",
        "icy",
        "idle",
        "inner",
        "jolly",
        "keen",
        "kind",
        "lively",
        "lonely",
        "loud",
        "lucky",
        "magic",
        "merry",
        "mighty",
        "misty",
        "modern",
        "mystic",
        "narrow",
        "neat",
        "nimble",
        "noble",
        "odd",
        "pale",
        "plain",
        "polite",
        "proud",
        "pure",
        "quick",
        "quiet",
        "rapid",
        "rare",
        "rigid",
        "rough",
        "round",
        "royal",
        "rustic",
        "sharp",
        "shiny",
        "silent",
        "sleek",
        "slim",
        "sly",
        "smooth",
        "snowy",
        "soft",
        "spicy",
        "steady",
        "steep",
        "swift",
        "tiny",
        "vivid",
        "whimsical",
    ]
    nouns = [
        "badger",
        "beacon",
        "bear",
        "bison",
        "bolt",
        "brook",
        "candle",
        "cedar",
        "cliff",
        "cloud",
        "cobra",
        "comet",
        "condor",
        "coral",
        "crane",
        "creek",
        "crow",
        "dawn",
        "dingo",
        "dove",
        "dragon",
        "drift",
        "eagle",
        "ember",
        "falcon",
        "fern",
        "finch",
        "flame",
        "flint",
        "forge",
        "fox",
        "frost",
        "gazelle",
        "glacier",
        "glyph",
        "grove",
        "hawk",
        "heron",
        "hornet",
        "husky",
        "iris",
        "jackal",
        "jade",
        "jaguar",
        "kite",
        "koala",
        "lark",
        "lemur",
        "leopard",
        "lotus",
        "lynx",
        "maple",
        "marten",
        "meadow",
        "minnow",
        "moon",
        "moose",
        "moth",
        "narwhal",
        "newt",
        "oak",
        "orbit",
        "orchid",
        "osprey",
        "otter",
        "owl",
        "panda",
        "peak",
        "pebble",
        "pine",
        "plover",
        "pond",
        "prism",
        "quail",
        "raven",
        "reef",
        "ridge",
        "robin",
        "sage",
        "salmon",
        "seal",
        "seed",
        "shade",
        "shark",
        "slate",
        "snipe",
        "spark",
        "sphinx",
        "squid",
        "stork",
        "stream",
        "summit",
        "swan",
        "thorn",
        "tiger",
        "trout",
        "viper",
        "walrus",
        "wasp",
        "whale",
        "wolf",
        "wren",
        "zenith",
    ]
    return f"{random.choice(adjectives)}{sep}{random.choice(nouns)}"
