"""
Sequence pileup benchmark:
  1. Generate a random 250nt base sequence.
  2. Generate 50 000 mutated sequences (~30 SNPs, ~3 deletions, ~3 insertions each).
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
AVG_INSERTIONS = 3
AVG_DELETIONS = 3


def random_sequence(length: int, rng: random.Random) -> str:
    return "".join(rng.choices(BASES, k=length))


def mutate_sequence(
    seq: str,
    avg_mutations: int,
    avg_insertions: int,
    avg_deletions: int,
    rng: random.Random,
) -> str:
    """Return a copy of *seq* with Poisson SNPs, deletions, and insertions."""
    seq_list = list(seq)

    # SNPs
    n_muts = np.random.poisson(avg_mutations)
    snp_positions = rng.sample(range(len(seq_list)), min(n_muts, len(seq_list)))
    for pos in snp_positions:
        alt_bases = [b for b in BASES if b != seq_list[pos]]
        seq_list[pos] = rng.choice(alt_bases)

    # Deletions — remove positions high-to-low so earlier indices stay valid
    n_dels = np.random.poisson(avg_deletions)
    if n_dels > 0:
        del_positions = sorted(
            rng.sample(range(len(seq_list)), min(n_dels, len(seq_list))),
            reverse=True,
        )
        for pos in del_positions:
            del seq_list[pos]

    # Insertions — insert random bases at random positions
    n_ins = np.random.poisson(avg_insertions)
    for _ in range(n_ins):
        pos = rng.randint(0, len(seq_list))
        seq_list.insert(pos, rng.choice(BASES))

    return "".join(seq_list)


rng = random.Random(42)
np.random.seed(42)

print("Generating sequences …")
base_seq = random_sequence(SEQ_LEN, rng)
mutated_seqs = [
    mutate_sequence(base_seq, AVG_MUTATIONS, AVG_INSERTIONS, AVG_DELETIONS, rng)
    for _ in range(N_SEQS)
]
seq_lengths = [len(s) for s in mutated_seqs]
print(f"  Base sequence length      : {SEQ_LEN} nt")
print(f"  Number of sequences       : {N_SEQS}")
print(f"  Mutated seq length range  : {min(seq_lengths)}–{max(seq_lengths)} nt")
print(f"  Mutated seq mean length   : {sum(seq_lengths)/N_SEQS:.1f} nt")


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

# Aggregate alignment event counts
ps_total_snps = sum(
    sum(1 for e in r["substitutions"] if e["type"] == "X") for r in tile_parasail
)
ps_total_ins = sum(
    sum(1 for e in r["substitutions"] if e["type"] == "I") for r in tile_parasail
)
ps_total_dels = sum(
    sum(1 for e in r["substitutions"] if e["type"] == "D") for r in tile_parasail
)
print(f"\n  SNPs  — total: {ps_total_snps:>8,}  per-read avg: {ps_total_snps / N_SEQS:.2f}")
print(f"  INS   — total: {ps_total_ins:>8,}  per-read avg: {ps_total_ins / N_SEQS:.2f}")
print(f"  DEL   — total: {ps_total_dels:>8,}  per-read avg: {ps_total_dels / N_SEQS:.2f}")

print("\nDone.")
