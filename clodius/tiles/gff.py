import functools as ft
import random

import clodius.tiles.bedfile as ctb
import pandas as pd
import polars as pl

from clodius.utils import get_file_compression
from clodius.models.gff_models import *
from clodius.tiles.tabix import df_single_tile
from clodius.utils import TILE_OPTIONS_CHAR
from clodius.tiles.tabix import load_tbi_idx, single_indexed_tile
from smart_open import open
from uuid import uuid4


def gff_chromsizes(filename):
    """Use the "regions" sections of a GFF file as the chromsizes."""
    if isinstance(filename, str):
        filename = open(filename, "rb")

    t = pd.read_csv(
        filename,
        header=None,
        delimiter="\t",
        comment="#",
        compression=get_file_compression(filename),
    )
    regions = t[t[2] == "region"]
    return pd.Series(regions[4].values, index=regions[0])


def row_to_bedlike(row, css, orig_columns):
    attrs = dict([x.split("=") for x in row[8].split(";")])

    ret = {
        "uid": row["ix"],
        "xStart": row["xStart"],
        "xEnd": row["xEnd"],
        "chrOffset": css[row[0]],
        "importance": random.random(),
        "fields": [row[0], row[3], row[4], attrs["Name"], "-", row[6]],
    }

    return ret


def tileset_info(filename, chromsizes=None, index_filename=None):
    """

    Return the bounds of this tileset. The bounds should encompass the entire
    width of this dataset.

    So how do we know what those are if we don't know chromsizes? We can assume
    that the file is enormous (e.g. has a width of 4 trillion) and rely on the
    browser to pass in a set of chromsizes
    """
    if chromsizes is None:
        chromsizes = gff_chromsizes(filename)

    return ctb.tileset_info(filename, chromsizes, index_filename)


def rows_to_genes(rows, css):
    """Convert a set of gff rows into gene annotations in BED12+3 format.

    From https://genome.ucsc.edu/FAQ/FAQformat.html#format1.7

    The format consists of the following:

    chrom - Name of the chromosome (or contig, scaffold, etc.).
    chromStart - The starting position of the feature in the chromosome or scaffold. The first base in a chromosome is numbered 0.
    chromEnd - The ending position of the feature in the chromosome or scaffold. The chromEnd base is not included in the display of the feature. For example, the first 100 bases of a chromosome are defined as chromStart=0, chromEnd=100, and span the bases numbered 0-99.
    name - Name given to a region (preferably unique). Use "." if no name is assigned.
    score - Indicates how dark the peak will be displayed in the browser (0-1000). If all scores were "0" when the data were submitted to the DCC, the DCC assigned scores 1-1000 based on signal value. Ideally the average signalValue per base spread is between 100-1000.
    strand - +/- to denote strand or orientation (whenever applicable). Use "." if no orientation is assigned.
    thickStart - The starting position at which the feature is drawn thickly. Not used in gappedPeak type, set to 0.
    thickEnd - The ending position at which the feature is drawn thickly. Not used in gappedPeak type, set to 0.
    itemRgb - An RGB value of the form R,G,B (e.g. 255,0,0). Not used in gappedPeak type, set to 0.
    blockCount - The number of blocks (exons) in the BED line.
    blockSizes - A comma-separated list of the block sizes. The number of items in this list should correspond to blockCount.
    blockStarts - A comma-separated list of block starts. The first value must be 0 and all of the blockStart positions should be calculated relative to chromStart. The number of items in this list should correspond to blockCount.
    signalValue - Measurement of overall (usually, average) enrichment for the region.
    pValue - Measurement of statistical significance (-log10). Use -1 if no pValue is assigned.
    qValue - Measurement of statistical significance using false discovery rate (-log10). Use -1 if no qValue is assigned.
    """
    # GFF file entries may have PARENT=<ID> hierarchies

    pass


def single_tile(filename, chromsizes, tsinfo, z, x, settings=None):
    if isinstance(filename, str):
        filename = open(filename, "rb")

    hash_ = ctb.ts_hash(filename, chromsizes)

    if settings is None:
        settings = {}
    # hash the loaded data table so that we don't have to read the entire thing
    # and calculate cumulative start and end positions
    val = ctb.cache.get(hash_)

    if val is None:
        t = pd.read_csv(
            filename,
            comment="#",
            header=None,
            delimiter="\t",
            compression=get_file_compression(filename),
        )
        t = t[t[2] == "gene"]

        orig_columns = t.columns
        css = chromsizes.cumsum().shift().fillna(0).to_dict()

        # xStart and xEnd are cumulative start and end positions calculated
        # as if the chromosomes are concatenated from end to end
        t["chromStart"] = t[0].map(lambda x: css[x])
        t["xStart"] = t["chromStart"] + t[3]
        t["xEnd"] = t["chromStart"] + t[4]
        t["ix"] = t.index

        val = {"rows": t, "orig_columns": orig_columns, "css": css}
        ctb.cache.set(hash_, val)

    t = val["rows"]
    orig_columns = val["orig_columns"]
    css = val["css"]

    tileStart = x * tsinfo["max_width"] / 2**z
    tileEnd = (x + 1) * tsinfo["max_width"] / 2**z

    t = t.query(f"xEnd >= {tileStart} & xStart <= {tileEnd}")
    MAX_PER_TILE = settings.get("MAX_BEDFILE_ENTRIES") or 1024

    t = t.sample(MAX_PER_TILE) if len(t) > MAX_PER_TILE else t

    ret = t.apply(
        ft.partial(row_to_bedlike, css=css, orig_columns=orig_columns), axis=1
    )
    return list(ret.values)


def convert_raw_to_gff_df(raw_data):
    """Convert table with 'raw' column containing GFF rows to dataframe format."""
    rows = []
    for item in raw_data.iter_rows(named=True):
        raw_line = item["raw"]
        parts = raw_line.split("\t")
        if len(parts) >= 9:
            rows.append(
                {
                    "seqid": parts[0],
                    "source": parts[1],
                    "type": parts[2],
                    "start": int(parts[3]),
                    "end": int(parts[4]),
                    "score": parts[5],
                    "strand": parts[6],
                    "phase": parts[7],
                    "attributes": parts[8],
                }
            )
    return pl.DataFrame(rows)


def parse_gff_to_models(filtered_df, settings=None):
    """Parse filtered GFF dataframe into gene and transcript models."""

    def parse_attributes(attr_str):
        if attr_str is None:
            return {}
        attrs = {}
        for item in attr_str.split(";"):
            if "=" in item:
                key, value = item.split("=", 1)
                attrs[key] = value
        return attrs

    genes = {}
    transcripts = {}
    pseudogenes = {}

    for row in filtered_df.iter_rows(named=True):
        # Map GFF columns: seqname, source, feature, start, end, score, strand, frame, attribute
        attrs = parse_attributes(row.get("attributes"))

        entity_data = {
            "id": attrs.get(
                "ID",
                f"{row.get('type')}_{row.get('start')}_{row.get('end')}",
            ),
            "chrom": row.get("seqid"),
            "start": row.get("start"),
            "end": row.get("end"),
            "strand": (
                row.get("strand") if row.get("strand") in ["+", "-", "."] else None
            ),
            "score": (
                float(row.get("score"))
                if row.get("score") is not None and row.get("score") != "."
                else None
            ),
            "phase": (
                int(row.get("phase"))
                if row.get("phase") is not None and row.get("phase") != "."
                else None
            ),
            "attributes": attrs,
        }

        feature_type = row.get("type")

        if feature_type == "gene":
            gene = Gene(
                **entity_data,
                gene_biotype=attrs.get("gene_biotype"),
                pseudo=attrs.get("pseudo") == "true",
            )
            genes[entity_data["id"]] = GeneModel(gene=gene)

        elif feature_type == "pseudogene":
            pseudogene = Pseudogene(**entity_data)
            pseudogenes[entity_data["id"]] = PseudogeneModel(pseudogene=pseudogene)

        elif feature_type == "mRNA":
            transcript = mRNA(**entity_data, parent_gene_id=attrs.get("Parent", ""))
            transcripts[entity_data["id"]] = transcript

        elif feature_type == "lnc_RNA":
            transcript = lnc_RNA(**entity_data, parent_gene_id=attrs.get("Parent", ""))
            transcripts[entity_data["id"]] = transcript

        elif feature_type == "primary_transcript":
            transcript = primary_transcript(
                **entity_data, parent_gene_id=attrs.get("Parent", "")
            )
            transcripts[entity_data["id"]] = transcript

        elif feature_type == "antisense_RNA":
            transcript = antisense_RNA(
                **entity_data, parent_gene_id=attrs.get("Parent", "")
            )
            transcripts[entity_data["id"]] = transcript

        elif feature_type == "snoRNA":
            transcript = snoRNA(**entity_data, parent_gene_id=attrs.get("Parent", ""))
            transcripts[entity_data["id"]] = transcript

        elif feature_type in [
            "tRNA",
            "rRNA",
            "snRNA",
            "SRP_RNA",
            "RNase_P_RNA",
            "RNase_MRP_RNA",
        ]:
            transcript_class = globals().get(feature_type, tRNA)
            transcript = transcript_class(
                **entity_data, parent_gene_id=attrs.get("Parent", "")
            )
            transcripts[entity_data["id"]] = transcript

        elif feature_type == "ncRNA":
            # Generic ncRNA - use lnc_RNA as fallback
            transcript = lnc_RNA(**entity_data, parent_gene_id=attrs.get("Parent", ""))
            transcripts[entity_data["id"]] = transcript

        elif feature_type == "miRNA":
            mirna = miRNA(**entity_data, parent_transcript_id=attrs.get("Parent", ""))
            parent_id = attrs.get("Parent", "")
            if parent_id in transcripts and hasattr(transcripts[parent_id], "mirnas"):
                transcripts[parent_id].mirnas.append(mirna)

        elif feature_type == "exon":
            exon = Exon(**entity_data)
            parent_id = attrs.get("Parent", "")
            if parent_id in transcripts:
                transcripts[parent_id].exons.append(exon)
            elif parent_id in pseudogenes:
                pseudogenes[parent_id].pseudogene.exons.append(exon)

        elif feature_type == "CDS":
            cds = CDS(**entity_data)
            parent_id = attrs.get("Parent", "")
            if parent_id in transcripts and isinstance(transcripts[parent_id], mRNA):
                transcripts[parent_id].cds.append(cds)
            else:
                if not parent_id:
                    parent_id = str(uuid4())
                # Create transcript of type "cds" if it doesn't exist
                transcript_data = entity_data.copy()
                transcript_data["id"] = parent_id
                transcript = mRNA(**transcript_data, parent_gene_id="")
                transcripts[parent_id] = transcript
                transcripts[parent_id].cds.append(cds)
                # Create exon from CDS
                exon = Exon(**entity_data)
                transcripts[parent_id].exons.append(exon)
                # Check if gene exists, create if it doesn't
                gene_id = attrs.get("gene_id") or f"gene_{parent_id}"
                if gene_id not in genes:
                    gene_data = entity_data.copy()
                    gene_data["id"] = gene_id
                    gene = Gene(**gene_data)
                    genes[gene_id] = GeneModel(gene=gene)
                transcripts[parent_id].parent_gene_id = gene_id

        # Skip unmodeled features: mobile_genetic_element, region, sequence_feature

    # Associate transcripts with genes
    for transcript in transcripts.values():
        if hasattr(transcript, "parent_gene_id") and transcript.parent_gene_id in genes:
            genes[transcript.parent_gene_id].transcripts.append(transcript)

    # Combine genes and pseudogenes
    all_genes = {**genes, **pseudogenes}

    for key in all_genes:
        all_genes[key] = all_genes[key].model_dump()
    for key in transcripts:
        transcripts[key] = transcripts[key].model_dump()

    return all_genes, transcripts


def tiles(filename, tile_ids, chromsizes=None, index_filename=None, settings=None):
    if chromsizes is None:
        chromsizes = gff_chromsizes(filename)

    def gff_single_tile_func(filename, chromsizes, tsinfo, z, x, settings=None):
        df = df_single_tile(
            filename=filename,
            chromsizes=chromsizes,
            tsinfo=tsinfo,
            z=z,
            x=x,
            mode="gff",
        )

        genes, transcripts = parse_gff_to_models(df)
        return {"genes": genes, "transcripts": transcripts}

    if isinstance(filename, str):
        file = open(filename, "rb", compression="disable")
    else:
        file = filename

    if isinstance(index_filename, str):
        index = open(index_filename, "rb", compression="disable")
    else:
        index = index_filename

    tile_values = []
    tsinfo = tileset_info(filename, chromsizes, index_filename)

    if index_filename:
        tbx_index = load_tbi_idx(index_filename)

    for tile_id in tile_ids:
        tile_option_parts = tile_id.split(TILE_OPTIONS_CHAR)[1:]
        tile_no_options = tile_id.split(TILE_OPTIONS_CHAR)[0]
        tile_id_parts = tile_no_options.split(".")
        tile_position = list(map(int, tile_id_parts[1:3]))
        tile_options = dict([o.split(":") for o in tile_option_parts])

        if len(tile_position) < 2:
            raise IndexError("Not enough tile info present")

        z, x = tile_position

        if index_filename:
            try:
                raw_data = single_indexed_tile(
                    file=file,
                    index=index,
                    chromsizes=chromsizes,
                    tsinfo=tsinfo,
                    z=z,
                    x=x,
                    tbx_index=tbx_index,
                )
                # for row in raw_data.iter_rows(named=True):
                #     print(row)
                converted_df = convert_raw_to_gff_df(raw_data)
                genes, transcripts = parse_gff_to_models(converted_df)
                values = {"genes": genes, "transcripts": transcripts}
            except ValueError as ve:
                values = {"error": str(ve)}
        else:
            values = gff_single_tile_func(
                file, chromsizes, tsinfo, z, x, settings=settings
            )

        tile_values += [(tile_id, values)]

    return tile_values
