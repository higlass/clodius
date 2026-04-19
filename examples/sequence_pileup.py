"""
Sequence pileup benchmark:
  1. Generate a random 250nt base sequence.
  2. Generate 20 000 mutated sequences (~30 SNPs each from the base).
  3. Build a pileup tile with parasail (profile-based, one profile per reference).
"""

import gzip
import json
import os
import time
import random
import numpy as np
import boto3

from clodius.tiles.pileup import get_pileup_alignment_data

OUT_DIR = os.path.expanduser("~/tmp")
S3_BUCKET = "petespocket"
S3_PREFIX = "tiles"
PRESIGN_SECONDS = 12 * 3600

s3 = boto3.client("s3")


def save_gz_json(path: str, data) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        json.dump(data, fh)
    print(f"  Saved → {path}")


def save_result(result: dict, prefix: str) -> None:
    """Write tileset_info and each tile to separate gzipped JSON files,
    upload each to S3, and print a 12-hour presigned URL.

    Files written:
      <prefix>_tileset_info.json.gz
      <prefix>_tile_<tile_id>.json.gz   (one per tile, dots replaced with _)
    """
    files = []

    tileset_info_path = os.path.join(OUT_DIR, f"{prefix}_tileset_info.json.gz")
    save_gz_json(tileset_info_path, result["tileset_info"])
    files.append(tileset_info_path)

    for tile_id, tile_data in result["tiles"].items():
        safe_id = tile_id.replace(".", "_")
        tile_path = os.path.join(OUT_DIR, f"{prefix}_tile_{safe_id}.json.gz")
        save_gz_json(tile_path, tile_data)
        files.append(tile_path)

    for local_path in files:
        filename = os.path.basename(local_path)
        s3_key = f"{S3_PREFIX}/{filename}"
        s3.upload_file(local_path, S3_BUCKET, s3_key)
        url = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": S3_BUCKET, "Key": s3_key},
            ExpiresIn=PRESIGN_SECONDS,
        )
        print(f"  {filename}\n  {url}")


# ---------------------------------------------------------------------------
# 1.  Data generation
# ---------------------------------------------------------------------------

BASES = list("ACGT")
SEQ_LEN = 250
N_SEQS = 50_000
AVG_MUTATIONS = 30


def random_sequence(length: int, rng: random.Random) -> str:
    return "".join(rng.choices(BASES, k=length))


def mutate_sequence(seq: str, avg_mutations: int, rng: random.Random) -> str:
    """Return a copy of *seq* with Poisson(avg_mutations) random SNPs."""
    n_muts = np.random.poisson(avg_mutations)
    positions = rng.sample(range(len(seq)), min(n_muts, len(seq)))
    seq_list = list(seq)
    for pos in positions:
        alt_bases = [b for b in BASES if b != seq_list[pos]]
        seq_list[pos] = rng.choice(alt_bases)
    return "".join(seq_list)


rng = random.Random(42)
np.random.seed(42)

print("Generating sequences …")
base_seq = random_sequence(SEQ_LEN, rng)
mutated_seqs = [mutate_sequence(base_seq, AVG_MUTATIONS, rng) for _ in range(N_SEQS)]
actual_avg = (
    sum(sum(a != b for a, b in zip(base_seq, s)) for s in mutated_seqs) / N_SEQS
)
print(f"  Base sequence length : {SEQ_LEN} nt")
print(f"  Number of sequences  : {N_SEQS}")
print(f"  Actual mean SNPs/seq : {actual_avg:.1f}")


# ---------------------------------------------------------------------------
# 2.  Parasail  (profile-based — build reference profile once, then batch)
# ---------------------------------------------------------------------------

print("\n--- Parasail (profile-based / batched) ---")
t0 = time.perf_counter()
result_parasail = get_pileup_alignment_data(base_seq, mutated_seqs, method="parasail")
t_parasail = time.perf_counter() - t0
tile_parasail = result_parasail["tiles"]["x.0.0"]
print(f"  Time : {t_parasail:.2f} s  ({t_parasail / N_SEQS * 1000:.2f} ms/seq)")
save_result(result_parasail, "pileup_parasail")

# Aggregate substitution counts
ps_total_subs = sum(len(r["substitutions"]) for r in tile_parasail)
print(f"\n  Total substitution entries : {ps_total_subs}")
print(f"  Per-read average           : {ps_total_subs / N_SEQS:.2f}")

print("\nDone.")
