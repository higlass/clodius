import unittest
from tempfile import TemporaryDirectory
import csv
from math import nan

import numpy as np
from numpy.testing import assert_array_equal
import h5py

from scripts.tsv_to_mrmatrix import coarsen, parse, get_height, get_width


class CoarsenTest(unittest.TestCase):
    def test_5_layer_pyramid(self):
        tile_size = 4
        max_zoom = 4
        max_width = tile_size * 2 ** max_zoom

        with TemporaryDirectory() as tmp_dir:
            hdf5 = h5py.File(tmp_dir + '/temp.hdf5', 'w')
            g = hdf5.create_group('resolutions')
            g1 = g.create_group('1')
            ds = g1.create_dataset('values', (max_width, max_width),
                                   dtype='f4', compression='lzf',
                                   fillvalue=np.nan)
            for y in range(max_width):
                a = np.array([float(x) for x in range(max_width)])
                ds[y, :max_width] = a

            # before coarsen()
            self.assertEqual(list(hdf5.keys()), ['resolutions'])
            self.assertEqual(list(hdf5['resolutions'].keys()), ['1'])
            self.assertEqual(list(hdf5['resolutions']['1'].keys()), ['values'])
            self.assertEqual(
                list(hdf5['resolutions']['1']['values'].shape), [64, 64])
            self.assertEqual(
                hdf5['resolutions']['1']['values'][:].tolist()[0],
                [float(x) for x in range(64)]
            )

            coarsen(hdf5, type='values', tile_size=tile_size)

            # after coarsen()
            self.assertEqual(list(hdf5.keys()), ['resolutions'])
            self.assertEqual(list(hdf5['resolutions'].keys()), [
                             '1', '16', '2', '4', '8'])
            self.assertEqual(
                list(hdf5['resolutions']['16'].keys()), ['values'])
            shapes = {
                '1': 64,
                '2': 32,
                '4': 16,
                '8': 8,
                '16': 4
            }
            for (k, v) in shapes.items():
                self.assertEqual(hdf5['resolutions'][k]
                                 ['values'].shape, (v, v))
            row = [1920, 6016, 10112, 14208]
            self.assertEqual(
                hdf5['resolutions']['16']['values'][:].tolist(),
                [row, row, row, row])

    def test_math(self):
        tile_size = 2
        max_zoom = 2
        max_width = tile_size * 2 ** max_zoom

        with TemporaryDirectory() as tmp_dir:
            hdf5 = h5py.File(tmp_dir + '/temp.hdf5', 'w')
            g = hdf5.create_group('resolutions')
            g1 = g.create_group('1')
            ds = g1.create_dataset('values', (max_width, max_width),
                                   dtype='f4', compression='lzf',
                                   fillvalue=np.nan)
            for y in range(max_width):
                a = np.array([float(x) for x in range(max_width)])
                ds[y, :max_width] = a

            coarsen(hdf5, type='values', tile_size=tile_size)

            # after coarsen()
            self.assertEqual(list(hdf5.keys()), ['resolutions'])
            self.assertEqual(list(hdf5['resolutions'].keys()), ['1', '2', '4'])

            shapes = {
                '1': 8,
                '2': 4,
                '4': 2
            }
            for (k, v) in shapes.items():
                self.assertEqual(hdf5['resolutions'][k]
                                 ['values'].shape, (v, v))

            row8 = list(range(8))
            assert_array_equal(
                hdf5['resolutions']['1']['values'],
                [row8 for _ in range(8)])

            row4 = [8 * x + 2 for x in range(4)]
            assert_array_equal(
                hdf5['resolutions']['2']['values'],
                [row4 for _ in range(4)])

            row2 = [24, 88]
            assert_array_equal(
                hdf5['resolutions']['4']['values'],
                [row2 for _ in range(2)])


class ParseTest(unittest.TestCase):
    def test_wide_labelled_square(self):
        with TemporaryDirectory() as tmp_dir:
            csv_path = tmp_dir + '/tmp.csv'
            with open(csv_path, 'w', newline='') as csv_file:
                writer = csv.writer(csv_file, delimiter='\t')
                # header:
                col_labels = ['col-{}'.format(x) for x in range(513)]
                writer.writerow(col_labels)
                # body:
                for y in range(0, 3):
                    writer.writerow(['row-{}'.format(y)] + [0] * 512)
                for y in range(3, 6):
                    writer.writerow(['row-{}'.format(y)] + [1] * 512)
                for y in range(6, 9):
                    writer.writerow(['row-{}'.format(y)] + [1, -1] * 256)
            csv_handle = open(csv_path, 'r')

            hdf5_path = tmp_dir + 'tmp.hdf5'
            hdf5_write_handle = h5py.File(hdf5_path, 'w')

            height = get_height(csv_path)
            width = get_width(csv_path, is_labelled=True)
            parse(csv_handle, hdf5_write_handle, height, width,
                  delimiter='\t', first_n=None, is_labelled=True)

            hdf5 = h5py.File(hdf5_path, 'r')
            self.assertEqual(list(hdf5.keys()),
                             ['col_labels', 'resolutions', 'row_labels'])
            self.assertEqual(list(hdf5['col_labels']),
                             col_labels[1:])
            self.assertEqual(list(hdf5['row_labels']),
                             ['row-{}'.format(r) for r in range(9)])

            self.assertEqual(list(hdf5['resolutions'].keys()),
                             ['1', '2', '2-nan_values'])

            self.assertEqual(list(hdf5['resolutions']['1'].keys()), [
                             'nan_values', 'values'])
            assert_array_equal(
                hdf5['resolutions']['1']['nan_values'], [[0] * 512] * 512
            )
            res_1 = hdf5['resolutions']['1']['values']
            assert_array_equal(res_1[0], [0] * 512)
            assert_array_equal(res_1[3], [1] * 512)
            assert_array_equal(res_1[6], [1, -1] * 256)
            assert_array_equal(res_1[9], [nan] * 512)

            self.assertEqual(list(hdf5['resolutions']['2'].keys()), ['values'])
            # TODO: We are missing nan_values at higher aggregations: Bug?
            # https://github.com/higlass/clodius/issues/62
            res_2 = hdf5['resolutions']['2']['values']
            assert_array_equal(res_2[0], [0] * 256)
            # Stradles the 0 and 1 rows
            assert_array_equal(res_2[1], [2] * 256)
            assert_array_equal(res_2[2], [4] * 256)
            assert_array_equal(res_2[3], [0] * 256)  # -1 and +1 cancel out
            assert_array_equal(res_2[4], [0] * 256)
            assert_array_equal(res_2[5], [0] * 256)
            assert_array_equal(res_2[6], [0] * 256)
            # TODO: We lose nan at higher aggregations.
            # https://github.com/higlass/clodius/issues/62

    def _assert_unlabelled_roundtrip_lt_256(
            self, matrix, delimiter):
        with TemporaryDirectory() as tmp_dir:
            csv_path = tmp_dir + '/tmp.csv'
            with open(csv_path, 'w', newline='') as csv_file:
                writer = csv.writer(csv_file, delimiter=delimiter)
                # body:
                for row in matrix:
                    writer.writerow(row)

            csv_handle = open(csv_path, 'r')
            hdf5_path = tmp_dir + 'tmp.hdf5'
            hdf5_write_handle = h5py.File(hdf5_path, 'w')

            is_labelled = False
            height = get_height(csv_path, is_labelled=is_labelled)
            width = get_width(csv_path, is_labelled=is_labelled)
            parse(csv_handle, hdf5_write_handle, height, width,
                  first_n=None, is_labelled=is_labelled,
                  delimiter=delimiter)

            hdf5 = h5py.File(hdf5_path, 'r')
            self.assertEqual(list(hdf5.keys()), ['resolutions'])
            self.assertEqual(list(hdf5['resolutions'].keys()), ['1'])
            self.assertEqual(list(hdf5['resolutions']['1'].keys()),
                             ['nan_values', 'values'])
            assert_array_equal(
                hdf5['resolutions']['1']['nan_values'],
                [[0] * len(matrix[0])] * len(matrix)
            )
            assert_array_equal(
                hdf5['resolutions']['1']['values'],
                matrix
            )

    def test_unlabelled_csv(self):
        self._assert_unlabelled_roundtrip_lt_256(
            matrix=[[x + y for x in range(4)] for y in range(4)],
            delimiter=','
        )

    def _assert_unlabelled_roundtrip_1024(
            self, matrix, first_row=None, first_col=None, first_n=None):
        delimiter = '\t'
        with TemporaryDirectory() as tmp_dir:
            csv_path = tmp_dir + '/tmp.csv'
            with open(csv_path, 'w', newline='') as csv_file:
                writer = csv.writer(csv_file, delimiter=delimiter)
                # body:
                for row in matrix:
                    writer.writerow(row)

            csv_handle = open(csv_path, 'r')
            hdf5_path = tmp_dir + 'tmp.hdf5'
            hdf5_write_handle = h5py.File(hdf5_path, 'w')

            is_labelled = False
            height = get_height(csv_path, is_labelled=is_labelled)
            width = get_width(csv_path, is_labelled=is_labelled)
            parse(csv_handle, hdf5_write_handle, height, width,
                  first_n=first_n, is_labelled=is_labelled,
                  delimiter=delimiter)

            hdf5 = h5py.File(hdf5_path, 'r')
            self.assertEqual(list(hdf5.keys()), ['resolutions'])
            self.assertEqual(list(hdf5['resolutions'].keys()),
                             ['1', '2', '2-nan_values', '4', '4-nan_values'])
            self.assertEqual(list(hdf5['resolutions']['1'].keys()),
                             ['nan_values', 'values'])
            self.assertEqual(list(hdf5['resolutions']['4'].keys()),
                             ['values'])
            res_4 = hdf5['resolutions']['4']['values']
            if first_row:
                assert_array_equal(res_4[0], first_row)
            if first_col:
                assert_array_equal(
                    [res_4[y][0] for y in range(len(first_col))],
                    first_col)

    def test_unlabelled_tsv_tall(self):
        self._assert_unlabelled_roundtrip_1024(
            matrix=[[1 for x in range(4)] for y in range(1000)],
            first_col=[16] * 250 + [0] * 6
        )

    def test_unlabelled_tsv_wide(self):
        self._assert_unlabelled_roundtrip_1024(
            matrix=[[1 for x in range(1000)] for y in range(4)],
            first_row=[16] * 250 + [0] * 6
        )

    def test_unlabelled_tsv_tall_first_n(self):
        self._assert_unlabelled_roundtrip_1024(
            matrix=[[1 for x in range(4)] for y in range(1000)],
            first_col=[8] + [0] * 255,
            first_n=2
        )

    def test_unlabelled_tsv_wide_first_n(self):
        self._assert_unlabelled_roundtrip_1024(
            matrix=[[1 for x in range(1000)] for y in range(4)],
            first_row=[8] * 250 + [0] * 6,
            first_n=2
        )
