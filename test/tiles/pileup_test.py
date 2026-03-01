import os.path as op

import pytest

pytest.importorskip("mappy")

from clodius.tiles.pileup import get_local_tiles
from clodius.alignment import align_sequences, alignment_to_subs, order_by_clustering


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


CSV_PATH = op.join("data", "pileup_test.csv")
REF_PATH = op.join("data", "pileup_ref.fa")
CHROMSIZES_PATH = op.join("data", "pileup_chromsizes.tsv")


def _assert_result_structure(result):
    assert "tilesetInfo" in result
    assert "tiles" in result
    tsinfo = result["tilesetInfo"]
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


def test_get_local_tiles_with_refrow():
    """get_local_tiles uses a CSV row as the reference sequence."""
    result = get_local_tiles(CSV_PATH, colname="seq", refrow=1)
    _assert_result_structure(result)
    tsinfo = result["tilesetInfo"]
    assert tsinfo["chromsizes"] == [["row_1", 60]]


def test_get_local_tiles_with_reffile_path():
    """get_local_tiles accepts a string filepath for the reference FASTA."""
    result = get_local_tiles(CSV_PATH, colname="seq", reffile=REF_PATH)
    _assert_result_structure(result)
    tsinfo = result["tilesetInfo"]
    assert tsinfo["chromsizes"] == [["ref1", 60]]


def test_get_local_tiles_with_reffile_object():
    """get_local_tiles accepts a binary file-like object for the reference FASTA."""
    with open(REF_PATH, "rb") as f:
        result = get_local_tiles(CSV_PATH, colname="seq", reffile=f)
    _assert_result_structure(result)


def test_get_local_tiles_with_chromsizes_path():
    """get_local_tiles accepts a string filepath for the chromsizes file."""
    result = get_local_tiles(
        CSV_PATH,
        colname="seq",
        reffile=REF_PATH,
        chromsizes_file=CHROMSIZES_PATH,
    )
    _assert_result_structure(result)
    tsinfo = result["tilesetInfo"]
    assert tsinfo["chromsizes"] == [["ref1", 60]]


def test_get_local_tiles_with_chromsizes_object():
    """get_local_tiles accepts a binary file-like object for the chromsizes file."""
    with open(CHROMSIZES_PATH, "rb") as f:
        result = get_local_tiles(
            CSV_PATH,
            colname="seq",
            reffile=REF_PATH,
            chromsizes_file=f,
        )
    _assert_result_structure(result)
    tsinfo = result["tilesetInfo"]
    assert tsinfo["chromsizes"] == [["ref1", 60]]


def test_get_local_tiles_substitution_detected():
    """The substitution in sample3 is reflected in the tile data."""
    result = get_local_tiles(CSV_PATH, colname="seq", reffile=REF_PATH)
    tile = result["tiles"]["x.0.0"]
    # At least one entry should have a substitution (sample3 differs at pos 20)
    all_subs = [s for entry in tile for s in entry["substitutions"]]
    mismatch_subs = [s for s in all_subs if s.get("type") == "X"]
    assert len(mismatch_subs) > 0
