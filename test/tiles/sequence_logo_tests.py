import unittest
import numpy as np
from unittest.mock import patch, MagicMock
from clodius.tiles.sequence_logos import tile_functions
from Bio.Seq import Seq
from Bio.SeqRecord import SeqRecord
from Bio.Align import MultipleSeqAlignment


class TestSequenceLogos(unittest.TestCase):

    def create_mock_alignment(self, sequences):
        """Create a mock alignment from sequences"""
        records = [SeqRecord(Seq(seq), id=f"seq{i}") for i, seq in enumerate(sequences)]
        return MultipleSeqAlignment(records)

    @patch("clodius.alignment.run_clustal_omega")
    def test_dna_sequences(self, mock_clustal):
        """Test tile_functions with DNA sequences"""
        sequences = ["ATCG", "ATGG", "ACCG"]
        mock_clustal.return_value = self.create_mock_alignment(sequences)
        result = tile_functions(sequences, seqtype="dna")

        # Check that we get the expected functions
        self.assertIn("tileset_info", result)
        self.assertIn("tiles", result)

        # Test tileset_info
        tsinfo = result["tileset_info"]()
        self.assertEqual(tsinfo["shape"], [4, 128])  # 4 DNA bases
        self.assertEqual(tsinfo["row_infos"], ["A", "C", "G", "T"])
        self.assertEqual(tsinfo["resolutions"], [1])

        # Test tiles function
        tile_data = result["tiles"](0, 0)
        self.assertIn("dense", tile_data)
        self.assertIn("dtype", tile_data)
        self.assertIn("shape", tile_data)
        self.assertEqual(tile_data["dtype"], "float16")
        self.assertEqual(tile_data["shape"], [4, 128])

    @patch("clodius.alignment.run_clustal_omega")
    def test_protein_sequences(self, mock_clustal):
        """Test tile_functions with protein sequences"""
        sequences = ["ACDE", "ACDF", "ACDG"]
        mock_clustal.return_value = self.create_mock_alignment(sequences)
        result = tile_functions(sequences, seqtype="protein")

        # Check that we get the expected functions
        self.assertIn("tileset_info", result)
        self.assertIn("tiles", result)

        # Test tileset_info
        tsinfo = result["tileset_info"]()
        self.assertEqual(tsinfo["shape"], [20, 128])  # 20 amino acids
        self.assertEqual(len(tsinfo["row_infos"]), 20)

        # Test tiles function
        tile_data = result["tiles"](0, 0)
        self.assertEqual(tile_data["shape"], [20, 128])

    @patch("clodius.alignment.run_clustal_omega")
    def test_invalid_seqtype(self, mock_clustal):
        """Test that invalid seqtype raises ValueError"""
        sequences = ["ATCG"]
        mock_clustal.return_value = self.create_mock_alignment(sequences)
        with self.assertRaises(ValueError):
            tile_functions(sequences, seqtype="invalid")

    @patch("clodius.alignment.run_clustal_omega")
    def test_empty_sequences(self, mock_clustal):
        """Test with empty sequences list"""
        sequences = []
        mock_clustal.return_value = self.create_mock_alignment(sequences)
        result = tile_functions(sequences, seqtype="dna")

        # Should still return valid structure
        self.assertIn("tileset_info", result)
        self.assertIn("tiles", result)


if __name__ == "__main__":
    unittest.main()
