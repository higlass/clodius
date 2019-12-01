from __future__ import print_function

import argparse
import os.path as op
import subprocess as sp
import tempfile as tf


def main():
    """
    python make_tiles.py input_file

    Create tiles for all of the entries in the JSON file.
    """
    parser = argparse.ArgumentParser()

    parser.add_argument("filepath")
    parser.add_argument("-a", "--assembly", default="hg19")
    parser.add_argument("-t", "--type", default="bedgraph")
    parser.add_argument(
        "--stdout",
        default=False,
        action="store_true",
        help="Dump output to stdout (not implemented yet)",
    )

    args = parser.parse_args()

    filedir = op.dirname(args.filepath)
    outfile = open(args.filepath + ".genome.sorted.gz", "w")

    tempfile = tf.TemporaryFile("w+b")

    if args.type == "bigwig":
        tempfile1 = tf.TemporaryFile()

        p05 = sp.Popen(
            ["bigWigToBedGraph", args.filepath, "/dev/fd/1"], stdout=tempfile1
        )
        p05.wait()
        tempfile1.seek(0)

        p0 = sp.Popen(
            ["pv", "-f", "-"],
            stdin=tempfile1,
            stdout=sp.PIPE,
            stderr=sp.PIPE,
            universal_newlines=True,
        )
        pn = p0
    elif args.type == "bedgraph":
        p0 = sp.Popen(
            ["pv", "-f", args.filepath],
            stdout=sp.PIPE,
            stderr=sp.PIPE,
            universal_newlines=True,
        )
        pn = p0

    p1 = sp.Popen(
        ["awk", "{print $1, $2, $1, $3, $4 }"], stdin=pn.stdout, stdout=sp.PIPE
    )
    p2 = sp.Popen(
        ["chr_pos_to_genome_pos.py", "-e 5", "-a", "{}".format(args.assembly)],
        stdin=p1.stdout,
        stdout=sp.PIPE,
    )
    p3 = sp.Popen(["sort", "-k1,1n", "-k2,2n", "-"], stdin=p2.stdout, stdout=tempfile)

    for line in iter(p0.stderr.readline, ""):
        print("line:", line.strip())

    p0.wait()
    p1.wait()
    p2.wait()
    p3.wait()

    tempfile.flush()
    print("tell:", tempfile.tell())
    tempfile.seek(0)

    p35 = sp.Popen(
        ["pv", "-f", "-"],
        stdin=tempfile,
        stdout=sp.PIPE,
        stderr=sp.PIPE,
        universal_newlines=True,
    )
    p4 = sp.Popen(["gzip"], stdin=p35.stdout, stdout=outfile)
    for line in iter(p35.stderr.readline, ""):
        print("line:", line.strip())

    p35.wait()
    p4.wait()

    print("filedir:", filedir)


if __name__ == "__main__":
    main()
