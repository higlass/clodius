from unittest import TestCase
import os
import sys

from scripts import conversion_wrapper

sys.path.append("cooler")

class ConversionWrapperTests(TestCase):

    def setUp(self):
        self.bigwig_input = ""
        self.gene_annotation_input = \
            "./sample_data/geneAnnotationsExonsUnions.short.bed"
        self.cooler_input = ""
        self.cooler_output = ""

    def test_wrapper_gene_annotation(self):
        sys.argv = ["--input_file {}".format(os.path.abspath(
            self.gene_annotation_input)), "--data_type gene_annotation"]
        conversion_wrapper.main()
