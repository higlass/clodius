import unittest
from tempfile import TemporaryDirectory
import csv
from math import nan

import numpy as np
from numpy.testing import assert_array_equal
import h5py

from scripts.tsv_to_mrmatrix import coarsen, parse

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
                    dtype='f4', compression='lzf', fillvalue=np.nan)
            for y in range(max_width):
                a = np.array([float(x) for x in range(max_width)])
                ds[y, :max_width] = a

            # before coarsen()
            self.assertEqual(list(hdf5.keys()), ['resolutions'])
            self.assertEqual(list(hdf5['resolutions'].keys()), ['1'])
            self.assertEqual(list(hdf5['resolutions']['1'].keys()), ['values'])
            self.assertEqual(list(hdf5['resolutions']['1']['values'].shape), [64, 64])
            self.assertEqual(
                hdf5['resolutions']['1']['values'][:].tolist()[0],
                [float(x) for x in range(64)]
            )

            coarsen(hdf5, tile_size=tile_size)

            # after coarsen()
            self.assertEqual(list(hdf5.keys()), ['resolutions'])
            self.assertEqual(list(hdf5['resolutions'].keys()), ['1', '16', '2', '4', '8'])
            self.assertEqual(list(hdf5['resolutions']['16'].keys()), ['values'])
            shapes = {
                '1': 64,
                '2': 32,
                '4': 16,
                '8': 8,
                '16': 4
            }
            for (k, v) in shapes.items():
                self.assertEqual(hdf5['resolutions'][k]['values'].shape, (v, v))
            row = [1920,  6016, 10112, 14208]
            self.assertEqual(
                hdf5['resolutions']['16']['values'][:].tolist(),
                [row, row, row, row])
            # TODO: Check the math

    def test_math(self):
        tile_size = 2
        max_zoom = 2
        max_width = tile_size * 2 ** max_zoom

        with TemporaryDirectory() as tmp_dir:
            hdf5 = h5py.File(tmp_dir + '/temp.hdf5', 'w')
            g = hdf5.create_group('resolutions')
            g1 = g.create_group('1')
            ds = g1.create_dataset('values', (max_width, max_width),
                    dtype='f4', compression='lzf', fillvalue=np.nan)
            for y in range(max_width):
                a = np.array([float(x) for x in range(max_width)])
                ds[y, :max_width] = a

            coarsen(hdf5, tile_size=tile_size)

            # after coarsen()
            self.assertEqual(list(hdf5.keys()), ['resolutions'])
            self.assertEqual(list(hdf5['resolutions'].keys()), ['1', '2', '4'])

            shapes = {
                '1': 8,
                '2': 4,
                '4': 2
            }
            for (k, v) in shapes.items():
                self.assertEqual(hdf5['resolutions'][k]['values'].shape, (v, v))

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
    def test_parse(self):
        with TemporaryDirectory() as tmp_dir:
            csv_path = tmp_dir + '/tmp.csv'
            with open(csv_path, 'w', newline='') as csv_file:
                writer = csv.writer(csv_file, delimiter='\t')
                # header:
                labels = ['col-{}'.format(x) for x in range(513)]
                writer.writerow(labels)
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

            parse(csv_handle, hdf5_write_handle)

            hdf5 = h5py.File(hdf5_path, 'r')
            self.assertEqual(list(hdf5.keys()), ['labels', 'resolutions'])
            self.assertEqual(list(hdf5['labels']), labels[1:])

            self.assertEqual(list(hdf5['resolutions'].keys()), ['1', '2'])

            self.assertEqual(list(hdf5['resolutions']['1'].keys()), ['nan_values', 'values'])
            assert_array_equal(
                hdf5['resolutions']['1']['nan_values'], [[0] * 512] * 512
            )
            res_1 = hdf5['resolutions']['1']['values']
            assert_array_equal(res_1[0], [0] * 512)
            assert_array_equal(res_1[3], [1] * 512)
            assert_array_equal(res_1[6], [1, -1] * 256)
            assert_array_equal(res_1[9], [nan] * 512)

            self.assertEqual(list(hdf5['resolutions']['2'].keys()), ['values'])
            res_2 = hdf5['resolutions']['2']['values']
            assert_array_equal(res_2[0], [0] * 256)
            assert_array_equal(res_2[1], [2] * 256) # Stradles the 0 and 1 rows
            assert_array_equal(res_2[2], [4] * 256)
            assert_array_equal(res_2[3], [0] * 256) # -1 and +1 cancel out
            assert_array_equal(res_2[4], [0] * 256)
            assert_array_equal(res_2[5], [0] * 256)
            assert_array_equal(res_2[6], [0] * 256)
            # TODO: We lose nan at higher aggregations:
            # Maybe regular mean/sum instead of treating missing values as 0?
