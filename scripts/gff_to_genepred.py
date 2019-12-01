#!/usr/bin/python

import random
import sys
import argparse


def dump_transcript(
    gene_name,
    gene_id,
    gene_type,
    gene_description,
    gene_importance,
    gene_start,
    gene_end,
    transcript_id,
    chrom,
    start,
    end,
    strand,
    cdss,
    exons,
):
    """
    Print out a set of transcripts for this gene
    """
    if int(end) < int(start):
        print("WARNING: end < start:", transcript_id, start, end, file=sys.stderr)

    print(
        "{chrom}\t{start}\t{end}\t{gene_name}\t{importance}\t{strand}\t{transcript_id}\t{gene_id}\t{gene_type}\t{gene_description}\t{cds_start}\t{cds_end}\t{exon_starts}\t{exon_ends}".format(
            chrom=chrom,
            start=gene_start,
            end=gene_end,
            gene_name=gene_name,
            importance=gene_importance,
            strand=strand,
            transcript_id=transcript_id,
            gene_id=gene_id,
            gene_type=gene_type,
            gene_description=gene_description,
            cds_start=start,
            cds_end=end,
            exon_starts=",".join([str(e[1]) for e in exons]),
            exon_ends=",".join([str(e[2]) for e in exons]),
        )
    )


def main():
    parser = argparse.ArgumentParser(
        description="""

    python gff_to_gene_pred.py
"""
    )

    parser.add_argument("gff_file")
    parser.add_argument(
        "--save-chromsizes",
        default=None,
        help="Store the chromsizes in a separate file",
        type=str,
    )
    # parser.add_argument('-o', '--options', default='yo',
    # help="Some option", type='str')
    # parser.add_argument('-u', '--useless', action='store_true',
    # help='Another useless option')

    args = parser.parse_args()

    counter = 0
    with open(args.gff_file, "r") as f:
        transcript_id = None
        chromsizes = []

        for line in f:
            counter += 1
            if line.strip()[0] == "#":
                # comment line
                continue

            parts = line.strip().split("\t")
            # print('parts:', parts)

            chrom = parts[0]
            # annotation_from = parts[1]
            annotation_type = parts[2]
            start_pos = int(parts[3])
            end_pos = int(parts[4])
            strand = parts[6]
            to_split = parts[8]

            """
            new_string = []
            in_quote = False

            # remove any semicolons when are within quoted
            # strings
            for char in to_split:
                if char == '"':
                    if in_quote is False:
                        in_quote = True
                    else:
                        in_quote = False

                if in_quote and char == ';':
                    char = ','

                new_string += [char]

            new_string = "".join(new_string)
            """

            """
            print("line:", line)
            print('parts:', parts[8])
            my_splitter = shlex.shlex(
                parts[8], posix=True
            )
            my_splitter.whitespace = ';'
            my_splitter.whitespace_split = True
            print('split_parts:', split_parts)
            """
            split_parts = to_split.split(";")

            attrs = {}
            for x in split_parts:
                try:
                    x_split = x.split("=")
                    attrs[x_split[0]] = x_split[1]
                except IndexError as ve:
                    print("WARNING: Strange Parts:", to_split, ve, file=sys.stderr)

            if annotation_type == "chromosome":
                id_parts = attrs["ID"].split(":")
                chromname = id_parts[0] if len(id_parts) == 1 else id_parts[1]
                chromsize = end_pos

                chromsizes += [(chromname, chromsize)]

            if annotation_type == "gene" or annotation_type == "tRNA_gene":
                if transcript_id is not None:
                    pass
                    # TODO: Fill in these variables
                    # dump_transcript(gene_name,
                    #                 gene_id,
                    #                 gene_type,
                    #                 gene_description,
                    #                 gene_importance,
                    #                 gene_start,
                    #                 gene_end,
                    #                 transcript_id,
                    #                 transcript_chrom,
                    #                 transcript_start,
                    #                 transcript_end,
                    #                 transcript_strand,
                    #                 transcript_cdss,
                    #                 transcript_exons)

                split_id = attrs["ID"].split(":")
                gene_id = attrs["ID"]

                if "GENE_NAME" in attrs:
                    gene_name = attrs["GENE_NAME"]
                elif "Name" in attrs:
                    split_name = attrs["Name"].split(":")
                    print("split_name", split_name, file=sys.stderr)
                    gene_name = split_name[0] if len(split_name) == 1 else split_name[1]
                else:
                    gene_name = split_id[0] if len(split_id) == 1 else split_id[1]
                    print("WARNING: no gene name:", to_split, file=sys.stderr)

                if "GENE_TYPE" in attrs:
                    gene_type = attrs["GENE_TYPE"]
                elif "biotype" in attrs:
                    gene_type = attrs["biotype"]
                else:
                    print(
                        "WARNING: no gene type (GENE_TYPE or biotype attribute)",
                        to_split,
                        file=sys.stderr,
                    )

                gene_description = (
                    attrs["description"] if "description" in attrs else "-"
                )
                gene_importance = random.randint(0, 10000)
                gene_start = start_pos
                gene_end = end_pos

                # set this to none so we don't try to dump a non-existent
                # transcript next time we encounter one
                transcript_id = None

                # we'll pre-set these values
                """
                transcript_exons = []
                transcript_gene_name = gene_name
                transcript_start = gene_start
                transcript_end = gene_end
                """

            if annotation_type == "transcript" or annotation_type == "mRNA":
                if transcript_id is not None:
                    dump_transcript(
                        gene_name,
                        gene_id,
                        gene_type,
                        gene_description,
                        gene_importance,
                        gene_start,
                        gene_end,
                        transcript_id,
                    )
                # transcript_chrom,
                # transcript_start,
                # transcript_end,
                # transcript_strand,
                # transcript_cdss,
                # transcript_exons)

                transcript_exons = []
                transcript_id = attrs["ID"]
                # transcript_gene_name = gene_name
                transcript_chrom = chrom
                transcript_start = start_pos
                transcript_end = end_pos
                transcript_strand = strand
                transcript_cdss = []

            if annotation_type == "exon":
                parent_id = attrs["Parent"]
                if parent_id != transcript_id:
                    print(
                        "Exon parent doesn't match transcript_id",
                        parent_id,
                        transcript_id,
                        file=sys.stderr,
                    )
                transcript_exons += [(chrom, start_pos, end_pos)]

    dump_transcript(
        gene_name,
        gene_id,
        gene_type,
        gene_description,
        gene_importance,
        gene_start,
        gene_end,
        transcript_id,
        transcript_chrom,
        transcript_start,
        transcript_end,
        transcript_strand,
        transcript_cdss,
        transcript_exons,
    )

    if args.save_chromsizes:
        with open(args.save_chromsizes, "w") as f:
            for (name, size) in chromsizes:
                f.write("{}\t{}\n".format(name, size))


if __name__ == "__main__":
    main()
