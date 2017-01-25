from __future__ import print_function

__author__ = "Alaleh Azhir,Peter Kerpedjiev"

#!/usr/bin/python

import collections as col
import sys
import argparse

def main():
    parser = argparse.ArgumentParser(description="""
    
    python ExonUnion.py Calculate the union of the exons of a list
    of transcript.

    chr10   27035524        27150016        ABI1    76      -       NM_001178120    10006   protein-coding  abl-interactor 1        27037498        27149792        10      27035524,27040526,27047990,27054146,27057780,27059173,27060003,27065993,27112066,27149675,      27037674,27040712,27048164,27054247,27057921,27059274,27060018,27066170,27112234,27150016,
""")

    parser.add_argument('transcript_bed')
    #parser.add_argument('-o', '--options', default='yo',
    #					 help="Some option", type='str')
    #parser.add_argument('-u', '--useless', action='store_true', 
    #					 help='Another useless option')
    args = parser.parse_args()

    inputFile = open(args.transcript_bed, 'r')

    # a dictionary to hold sets of tuples of exons, indexed by geneId
    exonUnions = col.defaultdict(set)

    # For each gene, we'll maintain the minimum and maximum transcription
    # start and end site, respectively
    txStarts = col.defaultdict(lambda: sys.maxint)
    txEnds = col.defaultdict(lambda: -sys.maxint)

    # do the same thing with coding sequence starts and ends
    cdsStarts = col.defaultdict(lambda: sys.maxint)
    cdsEnds = col.defaultdict(lambda: -sys.maxint)

    # gene types, chromosomes, strands, names, scores and descriptions should be the same across 
    # all transcripts for a gene
    geneDescs = {}
    geneTypes = {}
    geneStrands = {}
    geneScores = {}
    geneChrs = {}
    geneNames = {}


    for line in inputFile:
        words = line.strip().split("\t")

        chrName = words[0]
        txStart = words[1]
        txEnd = words[2]
        geneName = words[3]
        score = words[4]
        strand = words[5]
        refseqId = words[6]
        geneId = words[7]
        geneType = words[8]
        geneDesc = words[9]
        cdsStart = words[10]
        cdsEnd = words[11]
        exonStarts = words[12]
        exonEnds = words[13]

        txStarts[geneId] = min(txStarts[geneId], int(txStart))
        txEnds[geneId] = max(txEnds[geneId], int(txEnd))

        cdsStarts[geneId] = min(cdsStarts[geneId], int(cdsStart))
        cdsEnds[geneId] = max(cdsEnds[geneId], int(cdsEnd))

        geneDescs[geneId] = geneDesc
        geneStrands[geneId] = strand
        geneScores[geneId] = score
        geneTypes[geneId] = geneType
        geneChrs[geneId] = chrName
        geneNames[geneId] = geneName


        # for some reason, exon starts and ends have trailing commas
        exonStartParts = exonStarts.strip(",").split(',')
        exonEndParts = exonEnds.strip(",").split(',')

        # add each exon to this gene's set of exon unions
        for exonStart,exonEnd in zip(exonStartParts, exonEndParts):
            exonUnions[geneId].add((int(exonStart), int(exonEnd)))

    
    for geneId in exonUnions:
        output = "\t".join(map(str, [geneChrs[geneId], txStarts[geneId], txEnds[geneId],
                            geneNames[geneId], geneScores[geneId], geneStrands[geneId],
                            'union_' + geneId, geneId, geneTypes[geneId], geneDescs[geneId],
                            cdsStarts[geneId], cdsEnds[geneId], 
                            ",".join([str(e[0]) for e in sorted(exonUnions[geneId])]),
                            ",".join([str(e[1]) for e in sorted(exonUnions[geneId])])]))
        print(output)



if __name__ == '__main__':
    main()



