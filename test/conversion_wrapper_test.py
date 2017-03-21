from unittest import TestCase
import os
from os.path import abspath
import os.path as op
import sys

from scripts import conversion_wrapper
from scripts.conversion_wrapper import format_output_filename
from bedfile_test import check_table
from bigwig_test import check_tileset_info
from utils import get_cooler_info


class ConversionWrapperTests(TestCase):

    def setUp(self):
        self.bigwig_input = abspath("test/sample_data/test1.bw")
        self.gene_annotation_input = abspath(
            "test/sample_data/geneAnnotationsExonsUnions.short.bed")
        self.cooler_input = abspath(
            "test/sample_data/Dixon2012-J1-NcoI-R1-filtered.1000kb.cool")

    def tearDown(self):
        try:
            os.remove("geneAnnotationsExonsUnions.short.multires.bed")
            os.remove("test1.multires.bw")
            os.remove(
                "Dixon2012-J1-NcoI-R1-filtered.1000kb.multires.cool")
        except OSError:
            pass

    def test_format_output_filename(self):
        self.assertEqual(
            op.splitext(self.bigwig_input)[0] + ".multires.bw",
            format_output_filename(self.bigwig_input, "bigwig")
        )
        self.assertEqual(
            op.splitext(self.gene_annotation_input)[0] + ".multires.bed",
            format_output_filename(
                self.gene_annotation_input, "gene_annotation")
        )
        self.assertEqual(
            op.splitext(self.cooler_input)[0] + ".multires.cool",
            format_output_filename(self.cooler_input, "cooler")
        )

    def test_wrapper_gene_annotation(self):
        sys.argv = ["fake.py", "--input-file", self.gene_annotation_input,
                    "--filetype", "gene_annotation", "--assembly", "hg19"]
        conversion_wrapper.main()
        check_table(format_output_filename(self.gene_annotation_input, 'gene_annotation'))

    def test_wrapper_bigwig(self):
        sys.argv = ["fake.py", "--input-file", self.bigwig_input,
                    "--filetype", "bigwig", "--assembly", "hg19"]
        conversion_wrapper.main()
        check_tileset_info(format_output_filename(self.bigwig_input, 'bigwig'))

    def test_wrapper_cooler(self):
        sys.argv = ["fake.py", "--input-file", self.cooler_input,
                    "--filetype", "cooler"]
        conversion_wrapper.main()
        get_cooler_info(format_output_filename(self.cooler_input, 'cooler'))

    def test_wrapper_cooler_multiple_cpus(self):
        sys.argv = ["fake.py", "--input-file", self.cooler_input,
                    "--filetype", "cooler", "--n-cpus", "2"]
        conversion_wrapper.main()
        get_cooler_info(format_output_filename(self.cooler_input, 'cooler'))
