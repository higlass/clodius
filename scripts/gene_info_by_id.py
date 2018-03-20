#!/usr/bin/python

import datetime as dt
import os.path as op
import re
import sys
import requests
import argparse
import xml.etree.ElementTree as ET

def main():
    usage = """
    python gene_info_by_id.py

    Retrieve gene information from Entrez gene given the gene id. For now this
    just retrieves the gene summary.
    """
    num_args= 0
    parser = argparse.ArgumentParser(usage=usage)

    parser.add_argument('gene_ids', nargs='*')
    #parser.add_option('-o', '--options', dest='some_option', default='yo', help="Place holder for a real option", type='str')
    #parser.add_option('-u', '--useless', dest='uselesss', default=False, action='store_true', help='Another useless option')
    parser.add_argument('-o', '--output-file', default=None, help="The file to store the downloaded gene information to")
    parser.add_argument('-f', '--id-list', default=None, help="A file containing a list of ids to retrieve information for")
    parser.add_argument('-e', '--exclude-id-list', default=None, help="A list of IDs to exclude")

    args = parser.parse_args()

    if len(args.gene_ids) < 1 and args.id_list is None:
        # no ids passed as argument and no id list file passed in
        parser.print_help()
        sys.exit(1)


    gene_ids = args.gene_ids
    if args.id_list is not None:
        with open(args.id_list, 'r') as f:
            for line in f:
                try:
                    gene_id = int(line.strip().split()[0])
                    gene_ids += [gene_id]
                except Exception as ex:
                    continue

    # We may have already downloaded a bunch of IDs and want to exclude them
    exclude_ids = set()
    if args.exclude_id_list is not None:
        with open(args.exclude_id_list, 'r') as f:
            for line in f:
                try:
                    gene_id = int(line.strip().split()[0])
                    exclude_ids.add(gene_id)
                except Exception as ex:
                    continue
    

    for gene_id in gene_ids:
        try:
            gene_id = int(gene_id)
        except Exception as ex:
            print("Gene id not a number as expected", file=sys.stderr)
            continue

        if gene_id in exclude_ids:
            print("Excluding...", gene_id, file=sys.stderr)
            continue

        link = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=gene&id={}&retmode=xml".format(gene_id)
        ret = requests.get(link)

        tree = ET.fromstring(ret.content)
        entrez_gene = tree.find('Entrezgene')
        
        #gene_type = entrez_gene.find('Entrezgene_type').get('value')
        #gene_desc = entrez_gene.find('Entrezgene_gene').find('Gene-ref').find('Gene-ref_desc').text
        try:
            gene_summary = entrez_gene.find('Entrezgene_summary').text
            print("{}\t{}".format(gene_id, gene_summary))
        except Exception as ex:
            print(ex, "gene_id:", gene_id, "ret_status:", ret.status_code, file=sys.stderr )
            print("{}\t".format(gene_id))
         

if __name__ == '__main__':
    main()

