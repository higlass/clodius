from unittest import TestCase
import os
from os.path import abspath
import sys

from scripts import conversion_wrapper
from scripts.conversion_wrapper import format_output_filename
from test_tile_bedfile import check_table
from test_tile_bigwig import check_tileset_info
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
            "test1.multires.bw",
            format_output_filename(self.bigwig_input, "bigwig")
        )
        self.assertEqual(
            "geneAnnotationsExonsUnions.short.multires.bed",
            format_output_filename(
                self.gene_annotation_input, "gene_annotation")
        )
        self.assertEqual(
            "Dixon2012-J1-NcoI-R1-filtered.1000kb.multires.cool",
            format_output_filename(self.cooler_input, "cooler")
        )

    def test_wrapper_gene_annotation(self):
        sys.argv = ["fake.py", "--input_file", self.gene_annotation_input,
                    "--data_type", "gene_annotation"]
        conversion_wrapper.main()
        check_table("geneAnnotationsExonsUnions.short.multires.bed")

    def test_wrapper_bigwig(self):
        sys.argv = ["fake.py", "--input_file", self.bigwig_input,
                    "--data_type", "bigwig"]
        conversion_wrapper.main()
        check_tileset_info("test1.multires.bw")

    def test_wrapper_cooler(self):
        sys.argv = ["fake.py", "--input_file", self.cooler_input,
                    "--data_type", "cooler"]
        conversion_wrapper.main()
        get_cooler_info(
            "Dixon2012-J1-NcoI-R1-filtered.1000kb.multires.cool")
