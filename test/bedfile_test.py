from __future__ import print_function

import click.testing as clt
import clodius.cli.aggregate as cca
import clodius.tiles.beddb as ctb
import os
import os.path as op
import sqlite3
import tempfile

testdir = op.realpath(op.dirname(__file__))


def test_nonstandard_chrom():
    filename = "test/sample_data/test_non_standard_chrom.bed"
    f = tempfile.NamedTemporaryFile(delete=False)

    ret = cca._bedfile(
        filename, f.name, "hg19", None, False, None, 100, 1024, None, None, 0
    )

    assert ret is None


def test_get_tileset_info():
    filename = "test/sample_data/gene_annotations.short.db"
    t = ctb.tileset_info(filename)

    assert t["zoom_step"] == 1
    assert t["max_length"] == 3137161264
    assert t["max_width"] > 4000000000
    assert t["max_width"] < 5000000000


def test_table_created():
    check_table("test/sample_data/gene_annotations.short.db")


def check_table(filename):
    conn = sqlite3.connect(filename)
    c = conn.cursor()

    rows = c.execute(
        "SELECT * from intervals,position_index "
        "where intervals.id=position_index.id "
        "and zoomLevel < 1 "
        "and rStartPos > 2400000000 "
        "and rEndPos < 2500000000"
    )
    counter = 0
    for row in rows:
        assert row[3] > 2400000000
        assert row[4] < 2500000000
        counter += 1

    assert counter > 0


def test_get_tiles():
    filename = "test/sample_data/gene_annotations.short.db"

    ctb.tiles(filename, ["x.18.169283"])
    # TODO: Make assertions about result
    # print("tiles:", tiles)
    # x = int(tiles[0]['xStart'])
    # fields = tiles[0]['fields']
    # TODO: Make assertions


def test_gene_annotations():
    runner = clt.CliRunner()
    input_file = op.join(testdir, "sample_data", "exon_unions_mm10.bed")
    f = tempfile.NamedTemporaryFile(delete=False)

    result = runner.invoke(
        cca.bedfile,
        [
            input_file,
            "--max-per-tile",
            "20",
            "--importance-column",
            "5",
            "--delimiter",
            "\t",
            "--assembly",
            "mm10",
            "--output-file",
            f.name,
        ],
    )

    # import traceback
    a, b, tb = result.exc_info
    """
    print("exc_info:", result.exc_info)
    print("result:", result)
    print("result.output", result.output)
    print("result.error", traceback.print_tb(tb))
    print("Exception:", a,b)
    """

    rows = ctb.tiles(f.name, ["x.0.0"])[0][1]
    assert len(rows) == 2

    rows = ctb.tiles(f.name, ["x.11.113"])[0][1]
    assert rows[0]["fields"][3] == "Lrp1b"

    rows = ctb.tiles(f.name, ["x.11.112"])[0][1]
    assert rows[0]["fields"][3] == "Lrp1b"


def test_random_importance():
    # check that when aggregating using random importance, all values that
    # are in a higher resolution tile are also in the lower resolution
    f = tempfile.NamedTemporaryFile(delete=False)

    runner = clt.CliRunner()
    input_file = op.join(testdir, "sample_data", "25435_PM15-000877_SM-7QK6O.seg")

    result = runner.invoke(
        cca.bedfile,
        [
            input_file,
            "--max-per-tile",
            "2",
            "--importance-column",
            "random",
            "--assembly",
            "b37",
            "--has-header",
            "--output-file",
            f.name,
        ],
    )

    # import traceback
    a, b, tb = result.exc_info
    """
    print("exc_info:", result.exc_info)
    print("result:", result)
    print("result.output", result.output)
    print("result.error", traceback.print_tb(tb))
    print("Exception:", a,b)
    """

    tsinfo = ctb.tileset_info(f.name)
    assert "version" in tsinfo
    assert len(tsinfo["header"]) > 0

    # check to make sure that tiles in the higher zoom levels
    # are all present in lower zoom levels
    found = {}

    for rect in ctb.tiles(f.name, ["x.5.15"])[0][1]:
        found[rect["xStart"]] = False

    for rect in ctb.tiles(f.name, ["x.6.30"])[0][1]:
        if rect["xStart"] in found:
            found[rect["xStart"]] = True

    for rect in ctb.tiles(f.name, ["x.6.31"])[0][1]:
        if rect["xStart"] in found:
            found[rect["xStart"]] = True

    for key, value in found.items():
        assert value

    pass


def test_no_chromosome_limit():
    f = tempfile.NamedTemporaryFile(delete=False)

    runner = clt.CliRunner()
    input_file = op.join(testdir, "sample_data", "geneAnnotationsExonsUnions.short.bed")

    result = runner.invoke(
        cca.bedfile,
        [
            input_file,
            "--max-per-tile",
            "60",
            "--importance-column",
            "5",
            "--assembly",
            "hg19",
            "--output-file",
            f.name,
        ],
    )

    # import traceback
    """
    print("exc_info:", result.exc_info)
    print("result:", result)
    print("result.output", result.output)
    print("result.error", traceback.print_tb(tb))
    print("Exception:", a,b)
    """
    a, b, tb = result.exc_info

    rows = ctb.tiles(f.name, ["x.0.0"])[0][1]
    foundOther = False

    for row in rows:
        if row["fields"][0] != "chr1":
            # print("row", row)
            assert row["xStart"] > 200000000
        if row["fields"][0] != "chr14":
            foundOther = True
        break
    # make sure there's chromosome other than 14 in the output
    assert foundOther

    os.remove(f.name)
    pass


def test_chromosome_limit():
    f = tempfile.NamedTemporaryFile(delete=False)

    runner = clt.CliRunner()
    input_file = op.join(testdir, "sample_data", "geneAnnotationsExonsUnions.short.bed")

    runner.invoke(
        cca.bedfile,
        [
            input_file,
            "--max-per-tile",
            "60",
            "--importance-column",
            "5",
            "--assembly",
            "hg19",
            "--chromosome",
            "chr14",
            "--output-file",
            f.name,
        ],
    )
    # TODO: Make assertions about result

    # print('output:', result.output, result)
    rows = ctb.tiles(f.name, ["x.0.0"])[0][1]

    for row in rows:
        assert row["fields"][0] == "chr14"

    os.remove(f.name)
    pass


def test_float_importance():
    f = tempfile.NamedTemporaryFile(delete=False)

    runner = clt.CliRunner()
    input_file = op.join(testdir, "sample_data", "test_float_importance.bed")

    runner.invoke(
        cca.bedfile,
        [
            input_file,
            "--max-per-tile",
            "2",
            "--importance-column",
            "4",
            "--assembly",
            "hg38",
            "--no-header",
            "--output-file",
            f.name,
        ],
    )
    # TODO: Make assertions about result
