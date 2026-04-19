import os.path as op

import pytest

from clodius.tiles.pileup import get_local_tiles, get_pileup_alignment_data, cigar_to_subs
from clodius.alignment import align_sequences, alignment_to_subs


# ---------------------------------------------------------------------------
# alignment_to_subs  (no external deps)
# ---------------------------------------------------------------------------

def test_alignment_to_subs():
    a = align_sequences("TTTTT", "AAAATTATTAAAA")
    print("")
    print(a)
    s = alignment_to_subs(a)

    print("s", s)

    assert s[2][0]["type"] == "I"
    assert s[2][0]["pos"] == 0
    assert s[2][0]["length"] == 4

    assert s[2][-1]["type"] == "I"
    assert s[2][-1]["pos"] == 5
    assert s[2][-1]["length"] == 4

    a = align_sequences("TTTTT", "TTATT")
    s = alignment_to_subs(a)

    # assert 1-based start positions and closed intervals
    assert s[0] == 1
    assert s[1] == 6
    assert s[2][0]["pos"] == 2  # subs are 0-based
    assert s[2][0]["base"] == "T"
    assert s[2][0]["variant"] == "A"

    a = align_sequences("TTTTT", "TTATTT")
    s = alignment_to_subs(a)

    assert s[0] == 1
    assert s[1] == 6
    assert s[2][0]["pos"] == 2
    assert s[2][0]["type"] == "I"
    assert s[2][0]["length"] == 1


# ---------------------------------------------------------------------------
# get_local_tiles  (requires mappy)
# ---------------------------------------------------------------------------

CSV_PATH = op.join("data", "pileup_test.csv")
REF_PATH = op.join("data", "pileup_ref.fa")
CHROMSIZES_PATH = op.join("data", "pileup_chromsizes.tsv")


def _assert_result_structure(result):
    assert "tilesetInfo" in result
    assert "tiles" in result
    tsinfo = result["tilesetInfo"]["x"]
    assert "resolutions" in tsinfo
    assert "chromsizes" in tsinfo
    assert "columns" in tsinfo
    # The single tile at zoom 0, position 0 should be present
    assert "x.0.0" in result["tiles"]
    tile = result["tiles"]["x.0.0"]
    assert isinstance(tile, list)
    assert len(tile) > 0
    for entry in tile:
        assert "from" in entry
        assert "to" in entry
        assert "substitutions" in entry


class TestGetLocalTiles:
    @pytest.fixture(autouse=True)
    def require_mappy(self):
        pytest.importorskip("mappy")

    def test_get_local_tiles_with_refrow(self):
        """get_local_tiles uses a CSV row as the reference sequence."""
        result = get_local_tiles(CSV_PATH, colname="seq", refrow=1)
        _assert_result_structure(result)
        tsinfo = result["tilesetInfo"]["x"]
        assert tsinfo["chromsizes"] == [["row_1", 60]]

    def test_get_local_tiles_with_reffile_path(self):
        """get_local_tiles accepts a string filepath for the reference FASTA."""
        result = get_local_tiles(CSV_PATH, colname="seq", reffile=REF_PATH)
        _assert_result_structure(result)
        tsinfo = result["tilesetInfo"]["x"]
        assert tsinfo["chromsizes"] == [["ref1", 60]]

    def test_get_local_tiles_with_reffile_object(self):
        """get_local_tiles accepts a binary file-like object for the reference FASTA."""
        with open(REF_PATH, "rb") as f:
            result = get_local_tiles(CSV_PATH, colname="seq", reffile=f)
        _assert_result_structure(result)

    def test_get_local_tiles_with_chromsizes_path(self):
        """get_local_tiles accepts a string filepath for the chromsizes file."""
        result = get_local_tiles(
            CSV_PATH,
            colname="seq",
            reffile=REF_PATH,
            chromsizes_file=CHROMSIZES_PATH,
        )
        _assert_result_structure(result)
        tsinfo = result["tilesetInfo"]["x"]
        assert tsinfo["chromsizes"] == [["ref1", 60]]

    def test_get_local_tiles_with_chromsizes_object(self):
        """get_local_tiles accepts a binary file-like object for the chromsizes file."""
        with open(CHROMSIZES_PATH, "rb") as f:
            result = get_local_tiles(
                CSV_PATH,
                colname="seq",
                reffile=REF_PATH,
                chromsizes_file=f,
            )
        _assert_result_structure(result)
        tsinfo = result["tilesetInfo"]["x"]
        assert tsinfo["chromsizes"] == [["ref1", 60]]

    def test_get_local_tiles_substitution_detected(self):
        """The substitution in sample3 is reflected in the tile data."""
        result = get_local_tiles(CSV_PATH, colname="seq", reffile=REF_PATH)
        tile = result["tiles"]["x.0.0"]
        # At least one entry should have a substitution (sample3 differs at pos 20)
        all_subs = [s for entry in tile for s in entry["substitutions"]]
        mismatch_subs = [s for s in all_subs if s.get("type") == "X"]
        assert len(mismatch_subs) > 0


# ---------------------------------------------------------------------------
# cigar_to_subs and parasail backend  (requires parasail)
# ---------------------------------------------------------------------------

class TestCigarToSubs:
    @pytest.fixture(autouse=True)
    def require_parasail(self):
        pytest.importorskip("parasail")

    def test_all_matches(self):
        start, end, subs = cigar_to_subs(b"5=", "ACGTA", "ACGTA")
        assert start == 1
        assert end == 5
        assert subs == []

    def test_single_mismatch(self):
        # ref TTTTT, query TTATT — mismatch at index 2
        start, end, subs = cigar_to_subs(b"2=1X2=", "TTTTT", "TTATT")
        assert start == 1
        assert end == 5
        assert len(subs) == 1
        s = subs[0]
        assert s["type"] == "X"
        assert s["pos"] == 2
        assert s["base"] == "T"
        assert s["variant"] == "A"
        assert s["length"] == 1

    def test_insertion(self):
        # ref ACGT, query ACXGT — one extra base inserted after position 2
        start, end, subs = cigar_to_subs(b"2=1I2=", "ACGT", "ACXGT")
        assert len(subs) == 1
        s = subs[0]
        assert s["type"] == "I"
        assert s["pos"] == 2
        assert s["length"] == 1

    def test_deletion(self):
        # ref ACGGT, query ACGT — one base deleted at position 2
        start, end, subs = cigar_to_subs(b"2=1D2=", "ACGGT", "ACGT")
        assert len(subs) == 1
        s = subs[0]
        assert s["type"] == "D"
        assert s["pos"] == 2
        assert s["length"] == 1

    def test_multiple_mismatches(self):
        start, end, subs = cigar_to_subs(b"1X3=1X", "ACGTA", "TCGTC")
        mismatch_positions = [s["pos"] for s in subs if s["type"] == "X"]
        assert mismatch_positions == [0, 4]


class TestGetPileupAlignmentDataParasail:
    @pytest.fixture(autouse=True)
    def require_parasail(self):
        pytest.importorskip("parasail")

    # Reference sequence and test sequences from pileup_test.csv / pileup_ref.fa
    REF = "GCAGTTTACAGCTATGACCTGATCAAGTCGAATCGTAGCCTGAATCGAGCTTAGCATGTC"
    # identical to ref
    SEQ_MATCH = "GCAGTTTACAGCTATGACCTGATCAAGTCGAATCGTAGCCTGAATCGAGCTTAGCATGTC"
    # one mismatch at 0-based position 19 (T→A), matching sample3 in the CSV
    SEQ_SUB = "GCAGTTTACAGCTATGACCAGATCAAGTCGAATCGTAGCCTGAATCGAGCTTAGCATGTC"

    def test_result_structure(self):
        result = get_pileup_alignment_data(self.REF, [self.SEQ_MATCH], method="parasail")
        assert "tileset_info" in result
        assert "tiles" in result
        assert "x.0.0" in result["tiles"]
        tile = result["tiles"]["x.0.0"]
        assert isinstance(tile, list)
        assert len(tile) == 1
        entry = tile[0]
        assert "from" in entry
        assert "to" in entry
        assert "substitutions" in entry

    def test_identical_sequence_no_subs(self):
        result = get_pileup_alignment_data(self.REF, [self.SEQ_MATCH], method="parasail")
        entry = result["tiles"]["x.0.0"][0]
        assert entry["substitutions"] == []

    def test_substitution_detected(self):
        result = get_pileup_alignment_data(self.REF, [self.SEQ_SUB], method="parasail")
        entry = result["tiles"]["x.0.0"][0]
        mismatch_subs = [s for s in entry["substitutions"] if s["type"] == "X"]
        assert len(mismatch_subs) == 1
        s = mismatch_subs[0]
        assert s["base"] == "T"
        assert s["variant"] == "A"

    def test_multiple_sequences(self):
        seqs = [self.SEQ_MATCH, self.SEQ_SUB, self.SEQ_MATCH]
        result = get_pileup_alignment_data(self.REF, seqs, method="parasail")
        tile = result["tiles"]["x.0.0"]
        assert len(tile) == 3
        # first and third are identical — no subs
        assert tile[0]["substitutions"] == []
        assert tile[2]["substitutions"] == []
        # second has one mismatch
        assert any(s["type"] == "X" for s in tile[1]["substitutions"])

    def test_tileset_info_fields(self):
        result = get_pileup_alignment_data(self.REF, [self.SEQ_MATCH], method="parasail")
        tsinfo = result["tileset_info"]
        assert tsinfo["tile_size"] == len(self.REF)
        assert tsinfo["format"] == "subs"
        assert tsinfo["chromsizes"] == [("ref", len(self.REF))]

    def test_read_ids(self):
        seqs = [self.SEQ_MATCH, self.SEQ_SUB]
        result = get_pileup_alignment_data(self.REF, seqs, method="parasail")
        tile = result["tiles"]["x.0.0"]
        assert tile[0]["id"] == "r0_ref"
        assert tile[1]["id"] == "r1_ref"
