import subprocess
from collections import Counter
from Bio import SeqIO, AlignIO, pairwise2
from Bio.Seq import Seq
from Bio.SeqRecord import SeqRecord
from Bio.Align import MultipleSeqAlignment
from typing import Literal, Optional
import tempfile
import os
from Bio import Align
from Bio.Seq import Seq
import numpy as np

DNA_ALPHABET = ["-", "A", "C", "G", "T"]
PROTEIN_ALPHABET = [
    "-",
    "A",
    "C",
    "D",
    "E",
    "F",
    "G",
    "H",
    "I",
    "K",
    "L",
    "M",
    "N",
    "P",
    "Q",
    "R",
    "S",
    "T",
    "V",
    "W",
    "Y",
]


def align_sequences(seq1, seq2):
    """Align two sequences using arbitrary alignment scores."""
    aligner = Align.PairwiseAligner()

    aligner.match_score = 1
    aligner.mismatch_score = -4
    aligner.open_gap_score = -6
    aligner.extend_gap_score = -1

    alignments = aligner.align(seq1, seq2)
    best_alignment = alignments[0]

    return best_alignment


def alignment_to_subs(alignment):
    """Convert a BioPython alignment object into "subs" that are
    compatible with the higlass pileup track.

    :param alignment: A BioPython alignment object
    :returns: A dictionary containing the start and end positions of
        the alignment, relative to the reference as well as all of the
        modifications. These take the form of:

        {
            "pos": # the position of the modification
            "type": # the type of modification, X for match or mismatch,
                    # D for deletion and I for insertion
            "length": # the length of the modification
            "base": # The moiety at the modification position. Only present for
                substitutions, not insertions or deletions.
            "variant": # The variant being mutated to. Only present for
                substitutions, not insertions or deletions.
        }
    """
    parts = []
    ttrue = 0
    tpos = 0
    qpos = 0

    start = 0
    end = 0
    aligneds = list(zip(alignment.aligned[0], alignment.aligned[1]))

    for i, ((ts, te), (qs, qe)) in enumerate(aligneds):
        ts, te, qs, qe = int(ts), int(te), int(qs), int(qe)

        if i == 0:
            # start position
            start = ts
            tpos = ts
            ttrue = 0
        if i == len(aligneds) - 1:
            # end position
            end = te

        if ts > tpos:
            parts += [{"pos": ttrue, "type": "D", "length": ts - tpos}]
            ttrue += ts - tpos
        if qs > qpos:
            parts += [{"pos": ttrue, "type": "I", "length": qs - qpos}]
        for i in range(te - ts):
            if alignment.target[ts + i] != alignment.query[qs + i]:
                parts += [
                    {
                        "pos": ttrue + i,
                        "type": "X",
                        "length": 1,
                        "base": alignment.target[ts + i],
                        "variant": alignment.query[qs + i],
                    }
                ]

        ttrue += te - ts
        tpos = te
        qpos = qe

    # Handle trailing insertion in query sequence
    query_len = len(alignment.query)
    if qpos < query_len:
        parts += [{"pos": ttrue, "type": "I", "length": query_len - qpos}]

    return start + 1, end + 1, parts


def run_clustal_omega(sequences, seq_ids=None, seqtype="dna"):
    """
    Align sequences with Clustal Omega.

    Args:
        sequences (list of str): Input nucleotide sequences (unaligned).
        seq_ids (list of str, optional): IDs for sequences (default: numbered).

    Returns:
        alignment (MultipleSeqAlignment): Biopython alignment object.
    """
    if seq_ids is None:
        seq_ids = [f"seq{i}" for i in range(len(sequences))]

    # Create temp fasta input file
    with tempfile.NamedTemporaryFile("w", delete=False) as fasta_file:
        input_fasta = fasta_file.name
        records = [
            SeqRecord(Seq(seq), id=sid, description="")
            for seq, sid in zip(sequences, seq_ids)
        ]
        SeqIO.write(records, fasta_file, "fasta")

    # Create temp output file
    output_fasta = input_fasta + "_aligned.fasta"

    params = [
        "clustalo",
        "-i",
        input_fasta,
        "-o",
        output_fasta,
        "--force",
        "--outfmt=fasta",
    ]

    if seqtype is not None:
        params += [f"--seqtype={seqtype.upper()}"]

    # Run Clustal Omega
    subprocess.run(
        params,
        check=True,
    )

    # Parse alignment
    alignment = AlignIO.read(output_fasta, "fasta")

    # Clean up
    os.remove(input_fasta)
    os.remove(output_fasta)

    return alignment


def refseq_alignment(sequences, refseq, seq_ids=None, seqtype=None):
    """
    Align sequences to a reference sequence using pairwise alignment.

    Args:
        sequences (list of str): Input sequences to align to reference.
        refseq (str): Reference sequence.
        seq_ids (list of str, optional): IDs for sequences (default: numbered).

    Returns:
        alignment (MultipleSeqAlignment): Biopython alignment object.
    """
    if seq_ids is None:
        seq_ids = [f"seq{i}" for i in range(len(sequences))]

    if seqtype is None:
        all_seqs = [refseq] + sequences
        seqtype = (
            "dna"
            if all(set(seq.upper()) <= set("ACGT") for seq in all_seqs)
            else "protein"
        )

    aligned_records = []

    # Add reference sequence first
    ref_record = SeqRecord(Seq(refseq), id="refseq", description="")
    aligned_records.append(ref_record)

    # Align each sequence to reference
    for seq, seq_id in zip(sequences, seq_ids):
        if seqtype == "dna":
            alignment = pairwise2.align.globalms(
                refseq, seq, 2, -1, -2, -0.5, one_alignment_only=True
            )[0]
        else:
            alignment = pairwise2.align.globalms(
                refseq, seq, 1, -1, -2, -0.5, one_alignment_only=True
            )[0]

        ref_aligned, seq_aligned = alignment.seqA, alignment.seqB

        # Extract positions corresponding to reference sequence
        aligned_seq = ""
        for ref_char, seq_char in zip(ref_aligned, seq_aligned):
            if ref_char != "-":
                aligned_seq += seq_char

        aligned_record = SeqRecord(Seq(aligned_seq), id=seq_id, description="")
        aligned_records.append(aligned_record)

    return MultipleSeqAlignment(aligned_records)


def make_pwm_from_alignment(
    alignment, pseudocount=0, seqtype: Literal["dna", "protein"] = "dna"
):
    """
    Build a PWM from an aligned set of sequences.

    Args:
        alignment (MultipleSeqAlignment): Aligned sequences.
        pseudocount (int): Pseudocount for smoothing.

    Returns:
        pwm (dict): Dictionary of {base: [probabilities]}.
    """
    if seqtype is None:
        all_chars = set()
        for record in alignment:
            all_chars.update(str(record.seq).upper())
        seqtype = "dna" if all_chars <= set("ACGT-") else "protein"

    alphabet = DNA_ALPHABET if seqtype == "dna" else PROTEIN_ALPHABET

    seq_length = alignment.get_alignment_length()
    pwm = {base: [] for base in alphabet}

    seqs = []
    for record in alignment:
        seqs.append(str(record.seq).upper())

    for pos in range(seq_length):
        column = [record.seq[pos] for record in alignment]
        counts = Counter(column)

        total = sum(counts.get(base, 0) + pseudocount for base in alphabet)
        for base in alphabet:
            prob = (counts.get(base, 0) + pseudocount) / total
            pwm[base].append(prob)

    # print(pwm.keys())
    # # print(pwm["M"])
    # for key in pwm:
    #     print(f"{key} {pwm[key][0]:.2} {pwm[key][1]:.2}")
    return pwm, seqs


def generate_pwm_from_sequences(
    sequences,
    seq_ids=None,
    pseudocount=0,
    seqtype: Optional[Literal["dna", "protein"]] = None,
    refseq=None,
):
    """
    Align sequences using Clustal Omega and generate a position weight matrix (PWM).

    Args:
        sequences (list of str): Input nucleotide sequences (unaligned).
        seq_ids (list of str, optional): Sequence IDs (default: numbered seq0, seq1, ...).
        pseudocount (int, optional): Pseudocount for PWM smoothing.
        refseq: Use a reference sequence for the alignment. If not set then create
            a MSA using clustalO

    Returns:
        pwm (dict): PWM as dictionary {base: [probabilities]}.
        seqs: The aligned sequences
    """
    if refseq:
        # A reference sequence is provided, use it for alignment
        alignment = refseq_alignment(sequences, refseq, seq_ids, seqtype)
    else:
        alignment = run_clustal_omega(sequences, seq_ids, seqtype=seqtype)

    pwm, seqs = make_pwm_from_alignment(
        alignment, pseudocount=pseudocount, seqtype=seqtype
    )
    return pwm, seqs


def create_distance_matrix(sequences):
    n = len(sequences)
    distance_matrix = np.zeros((n, n))
    from tqdm import tqdm

    for i in tqdm(range(n)):
        for j in range(i + 1, n):
            score = pairwise2.align.globalxx(
                sequences[i], sequences[j], score_only=True
            )

            # Convert similarity to distance
            distance = 150 - score  # Assuming 150 nt sequences
            distance_matrix[i, j] = distance
            distance_matrix[j, i] = distance

    return distance_matrix


def cluster_sequences(sequences):
    from scipy.cluster.hierarchy import linkage, dendrogram

    # Generate distance matrix
    dist_matrix = create_distance_matrix(sequences)

    # Perform hierarchical clustering
    linkage_matrix = linkage(dist_matrix, method="average")

    dplot = dendrogram(linkage_matrix, no_plot=True)
    return linkage_matrix, dplot["leaves"]


def order_by_clustering(sequences):
    _, order = cluster_sequences(sequences)
    return [sequences[i] for i in order]
