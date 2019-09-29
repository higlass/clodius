from concurrent.futures import ThreadPoolExecutor
from collections.abc import Mapping, Sequence
from functools import cmp_to_key, partial
import logging
import re

import numpy as np
import pandas as pd

from .format import format_dense_tile

logger = logging.getLogger(__name__)

NS_REGEX = re.compile(r'(\d+)', re.U)
MAX_THREADS = 4
TILE_SIZE = 1024


AGGREGATION_MODES = {
    'mean': {'name': 'Mean', 'value': 'mean'},
    'min':  {'name': 'Min', 'value': 'min'},
    'max': {'name': 'Max', 'value': 'max'},
    'std': {'name': 'Standard Deviation', 'value': 'std'},
}

RANGE_MODES = {
    'minMax': {'name': 'Min-Max', 'value': 'minMax'},
    'whisker': {'name': 'Whisker', 'value': 'whisker'},
}


def natsort_key(s, _NS_REGEX=NS_REGEX):
    return tuple(
        [int(x) if x.isdigit() else x for x in _NS_REGEX.split(s) if x]
    )


def natcmp(x, y):
    if x.find('_') >= 0:
        x_parts = x.split('_')
        if y.find('_') >= 0:
            # chr_1 vs chr_2
            y_parts = y.split('_')
            return natcmp(x_parts[1], y_parts[1])
        else:
            # chr_1 vs chr1
            # chr1 comes first
            return 1

    if y.find('_') >= 0:
        # chr1 vs chr_1
        # y comes second
        return -1

    x_parts = tuple(
        [int(a) if a.isdigit() else a for a in NS_REGEX.split(x) if a]
    )
    y_parts = tuple(
        [int(a) if a.isdigit() else a for a in NS_REGEX.split(y) if a]
    )

    # order of these parameters is purposefully reverse how they should be
    # ordered
    for key in ['m', 'y', 'x']:
        if key in y.lower():
            return -1
        if key in x.lower():
            return 1

    try:
        if x_parts < y_parts:
            return -1
        elif y_parts > x_parts:
            return 1
        else:
            return 0
    except TypeError:
        return 1


def natsorted(iterable):
    return sorted(iterable, key=cmp_to_key(natcmp))


def get_quadtree_depth(total_length, tile_size):
    min_tile_cover = np.ceil(total_length / tile_size)
    return int(np.ceil(np.log2(min_tile_cover)))


def get_zoom_resolutions(total_length, tile_size):
    return [2**x for x in
            range(get_quadtree_depth(total_length, tile_size) + 1)][::-1]


def abs2genomic(chromseries, start_pos, end_pos):
    abs_chrom_offsets = np.r_[0, np.cumsum(chromseries.values)]
    cid_lo, cid_hi = np.searchsorted(
        abs_chrom_offsets,
        [start_pos, end_pos],
        side='right') - 1
    rel_pos_lo = start_pos - abs_chrom_offsets[cid_lo]
    rel_pos_hi = end_pos - abs_chrom_offsets[cid_hi]
    start = rel_pos_lo
    for cid in range(cid_lo, cid_hi):
        yield cid, start, chromseries[cid]
        start = 0
    yield cid_hi, start, rel_pos_hi


def _normalize_chromsizes(chromsizes):
    if isinstance(chromsizes, (pd.Series, Mapping)):
        return [[chrom, int(size)] for chrom, size in chromsizes.items()]
    elif isinstance(chromsizes, Sequence):
        return [[item[0], item[1]] for item in chromsizes]
    return chromsizes


def _build_tile(fetch_impl, chromseries, binsize, agg_mode, range_mode,
                region):
    cid, start, end = region
    n_bins = int(np.ceil((end - start) / binsize))
    n_dim = 1

    if range_mode == 'minMax':
        n_dim = 2

    if range_mode == 'whisker':
        n_dim = 4

    x = np.zeros((n_bins, n_dim)) if n_dim > 1 else np.zeros(n_bins)

    try:
        chrom = chromseries.index[cid]
        clen = chromseries.values[cid]
        missing = np.nan

        if range_mode == 'minMax':
            x[:, 0] = fetch_impl(chrom, start, end, n_bins, missing, 'min')
            x[:, 1] = fetch_impl(chrom, start, end, n_bins, missing, 'max')
        elif range_mode == 'whisker':
            x[:, 0] = fetch_impl(chrom, start, end, n_bins, missing, 'min')
            x[:, 1] = fetch_impl(chrom, start, end, n_bins, missing, 'max')
            x[:, 2] = fetch_impl(chrom, start, end, n_bins, missing, 'mean')
            x[:, 3] = fetch_impl(chrom, start, end, n_bins, missing, 'std')
        else:
            x[:] = fetch_impl(chrom, start, end, n_bins, missing, agg_mode)

        # drop the very last bin if it is smaller than the binsize
        if end == clen and clen % binsize != 0:
            x = x[:-1]

    except IndexError:
        # beyond the range of the available chromosomes
        # probably means we've requested a range of absolute
        # coordinates that stretch beyond the end of the genome
        x[:] = np.nan

    except KeyError:
        # probably requested a chromosome that doesn't exist (e.g. chrM)
        x[:] = np.nan

    return x


class BbiTilesetEngine:
    def __init__(self, chromsizes):
        if chromsizes is None:
            self.chromsizes = self.get_chromsizes()
        else:
            self.chromsizes = _normalize_chromsizes(chromsizes)
        names, lengths = zip(*self.chromsizes)
        self.chromseries = pd.Series(lengths, index=names)
        total_length = sum(self.chromseries.values)
        self.resolutions = get_zoom_resolutions(total_length, TILE_SIZE)
        self.max_depth = get_quadtree_depth(total_length, TILE_SIZE)

    def get_chromsizes(self):
        raise NotImplementedError

    def fetch_interval(self, chrom, start, end, n_bins, missing, summary):
        raise NotImplementedError

    def get_tile(self, zoom_level, tile_pos, agg_mode, range_mode,
                 chromseries=None):
        # break up the tile at all chomosome boundaries, fetch the pieces,
        # then glue them back together
        tile_size = TILE_SIZE * 2 ** (self.max_depth - zoom_level)
        start_pos = tile_pos * tile_size
        end_pos = start_pos + tile_size
        regions = list(abs2genomic(self.chromseries, start_pos, end_pos))
        job = partial(
            _build_tile,
            self.fetch_interval,
            chromseries or self.chromseries,
            self.resolutions[zoom_level],
            agg_mode,
            range_mode)
        with ThreadPoolExecutor(max_workers=16) as executor:
            arrays = list(executor.map(job, regions))
        return np.concatenate(arrays)

    def tileset_info(self):
        max_zoom = self.max_depth
        return {
            'min_pos': [0],
            'max_pos': [TILE_SIZE * 2 ** max_zoom],
            'max_width': TILE_SIZE * 2 ** max_zoom,
            'tile_size': TILE_SIZE,
            'max_zoom': max_zoom,
            'chromsizes': self.chromsizes,
            'aggregation_modes': AGGREGATION_MODES,
            'range_modes': RANGE_MODES
        }


class PybbiTilesetEngine(BbiTilesetEngine):
    def __init__(self, bwpath, chromsizes=None):
        import bbi
        self.bwpath = bwpath
        self._chromsizes = bbi.chromsizes
        self._fetch = bbi.fetch
        super().__init__(chromsizes)

    def get_chromsizes(self):
        try:
            chromsizes = self._chromsizes(self.bwpath)
            chromosomes = natsorted(chromsizes.keys())
            data = [[c, int(chromsizes[c])] for c in chromosomes]
        except Exception as e:
            logger.error(e)
            raise RuntimeError(
                'Error loading chromsizes from bbi file: {}'.format(e))
        return data

    def fetch_interval(self, chrom, start, end, n_bins, missing, summary):
        return self._fetch(
            self.bwpath,
            chrom,
            start,
            end,
            n_bins,
            missing=missing,
            summary=summary
        )


class PybigwigTilesetEngine(BbiTilesetEngine):
    def __init__(self, bwpath, chromsizes=None):
        import pyBigWig
        self._f = pyBigWig.open(bwpath)
        super().__init__(chromsizes)

    def get_chromsizes(self):
        try:
            chromsizes = self._f.chroms()
            chromosomes = natsorted(chromsizes.keys())
            data = [[c, int(chromsizes[c])] for c in chromosomes]
        except Exception as e:
            logger.error(e)
            raise RuntimeError(
                'Error loading chromsizes from bbi file: {}'.format(e))
        return data

    def fetch_interval(self, chrom, start, end, n_bins, missing, summary):
        # exact=False; numpy=True not released yet
        return np.array(
            self._f.stats(
                chrom, start=start, end=end, nBins=n_bins, type=summary
            ),
            dtype=np.float64
        )


def get_engine(engine, bwpath, chromsizes):
    """Get the bigwig/bigbed backend implementation."""
    engine = engine.lower()

    if engine == "auto":
        for eng in ["pybbi", "pybigwig"]:
            try:
                return get_engine(eng, bwpath, chromsizes)
            except RuntimeError:
                pass
        else:
            raise RuntimeError("Please install either pybbi or pyBigWig")

    elif engine == "pybbi" or engine == "bbi":
        try:
            import bbi  # noqa
        except ImportError:
            raise RuntimeError("`pybbi` not installed")
        return PybbiTilesetEngine(bwpath, chromsizes)

    elif engine == "pybigwig":
        try:
            import pyBigWig  # noqa
        except ImportError:
            raise RuntimeError("`pyBigWig not installed")
        return PybigwigTilesetEngine(bwpath, chromsizes)

    else:
        raise ValueError(
            'Unsupported engine: "{0}".'.format(engine)
            + '  Valid choices include "pybbi" and "pybigwig".'
        )


def tiles(bwpath, tile_ids, engine='auto', chromsizes_map={}, chromsizes=None):
    """
    Generate tiles from a bigwig file.

    Parameters
    ----------
    tileset: tilesets.models.Tileset object
        The tileset that the tile ids should be retrieved from
    tile_ids: [str,...]
        A list of tile_ids (e.g. xyx.0.0) identifying the tiles
        to be retrieved
    chromsizes_map: {uid: []}
        A set of chromsizes listings corresponding to the parameters of the
        tile_ids. To be used if a chromsizes id is passed in with the tile id
        with the `|cos:id` tag in the tile id
    chromsizes: [[chrom, size],...]
        A 2d array containing chromosome names and sizes. Overrides the
        chromsizes in chromsizes_map

    Returns
    -------
    tile_list: [(tile_id, tile_data),...]
        A list of tile_id, tile_data tuples

    """
    e = get_engine(engine, bwpath, chromsizes)

    out = []
    for tile_id in tile_ids:
        tag, *options = tile_id.split('|')
        tile_opts = dict([opt.split(':') for opt in options])
        cs_id = tile_opts.get('cos')

        id_parts = tag.split('.')
        zoom_level, tile_pos = [int(p) for p in id_parts[1:3]]
        mode = id_parts[3] if len(id_parts) > 3 else None

        agg_mode = 'mean'
        range_mode = None
        if mode in AGGREGATION_MODES:
            agg_mode = mode
        elif mode in RANGE_MODES:
            range_mode = mode

        if chromsizes is None and cs_id is not None:
            chromsizes_override = chromsizes_map.get(cs_id)
        else:
            chromsizes_override = None

        dense = e.get_tile(
            zoom_level,
            tile_pos,
            agg_mode,
            range_mode,
            chromsizes_override
        )
        out.append((tile_id, format_dense_tile(dense)))

    return out


def tileset_info(bwpath, chromsizes=None, engine='auto'):
    e = get_engine(engine, bwpath, chromsizes)
    return e.tileset_info()
