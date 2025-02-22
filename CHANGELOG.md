v0.20.2

- Convert cooler chromsizes to int64 to prevent overflow error with recent versions of h5py and numpy

v0.20.1

- Remove use of deprecated `max_chunk` argument from cooler tile fetcher.

v0.20.0

- remove `numpy` from setup requires
- Use builtin `warnings` module instead of relying on alias from `numpy`.
- Replace instances of `.iteritems()` with `.items()`

v0.19.0

- Fix decoding error in multivec tileset info
- Allow JSON in multivec tileset info

v0.18.1

- Don't pin versions in requirements.txt

v0.18.0

- Added bigwigs_to_multivec command
- Bumped dask version
- Bumped pandas version

v0.17.1

- Fix narrow npmatrix tile fetching bug

v0.17.0

- Updated the BAM file fetcher to do more efficient substitution loading
- Include strand, cigars, and other metadata from reads
- Added tabix loader that can be estimated to estimate the size of data in a region of a BAM file
- Add FASTA tileset

v0.16.0

- Return `chromsizes` as a single array in beddb `tileset_info`
- [BREAKING] Remove the `chrom_names` and `chrom_sizes` fields in the beddb tileset info

v0.15.4

- Remove type=bool from bedpe aggregate function to fix "Got secondary option for non-boolean flag" error
- No default assembly

v0.15.2

- More informative error message when doing bedfile_to_multivec conversion

v0.15.1

- Added support for multivec `row_infos` stored under `/info/row_infos` as an hdf5 utf-8 string dataset.

v0.15.0

- Improve performance of `clodius aggregate bedpe` using sqlite batch inserts, transactions, and PRAGMAs
- Show default values for `clodius aggregate bedpe -h`
- Add short options to `clodius aggregate bedpe`
- Make `clodius aggregate bedpe --chromosome` actually do something
- For bedpedb 1D tiles, retrieve entries where either regions at least partially overlaps with a tile
- Harmonize bedpedb tile getter names

v0.14.3

- Small bug when retrieving tile 0.0 from bam files
- More accurate generation of multivec tiles that span across chromosomes

v0.14.2

- Make sure that chromsizes are serialized as ints

v0.14.1

- Natural ordering of bam file chromosomes

v0.14.0

- Add "name" field to `beddb` format

v0.13.1

- Fix returned header values

v0.13.0

- Ran black on the entire code base
- Introduced versions to beddb and bed2ddb files
- Added zoom level to the r-tree index

v0.12.0

- Added tile queries for bigBed files, using score data (if available) to threshold those elements returned, based on a specified or default maximum.

v0.11.5

- Add tiles function and test for multivec. It can be used in higlass-python and higlass server.

v0.11.4

- Modified bedfile_to_multivec conversion to retain lines in which the end coordinate is not a multiple of the resolution.
  It adds an extra bin for the remainder. It also displays an error when start coordinate is not multiple of resolution
- Fix bug to handle headers.
- Modified create_multivec_multires for states files to show only the contents of the first column of the row_infos file as the name of the state.

v0.11.3

- Maintenance: Switched from nose to pytest and added coverage reporting.
- Use columnar format for BAM reads return values

v0.11.2

- Added `max_tile_width` parameter to ct.bam.tiles() so that users can set a
  limit on how large of a region data is returned for

v0.11.0

- Added bamfile support

v0.10.12

- Simplified density tiles generator.
- Fix error with `npvector` tileset_info.
- Add `__version__`.

v0.10.11

- Calculate np.nan on the fly if not available for npvector tracks

v0.10.10

- Fix error in bedfile_to_multivec conversion when encountering value-less files
- Update the bedpe aggregator to fix the error in using a chromsizes file
- Make tsv_to_mrmatrix more flexible and add it to the exported scripts.
- Display more meaningful error messages when encountering unknown chromosomes or assemblies
- Removed redundant multivec function
- Removed obsolete bigwig function
- Make tsv_to_mrmatrix more flexible and add it to the exported
- Detect non-symmetric square coolers using the storage-mode metadata. Support for the symmetric property is retained for the legacy mcool format.

v0.10.7

- Changed bins_per_dimension in npvector.tileset_info to match the value in
  in npvector.tiles (1024)

v0.10.5

- Removed slugid decode

v0.10.2 (2019-02-06)

- new option to import a "states" file format, a bed file with categorical data, e.g. from chromHMM to multivec format.
- while converting a bed file to multivec, each segment can now be a multiple of base_resolution,
  rather than exactly match the base_resolution

v0.10.1 (2019-01-22)

- Removed a buggy print statement from the conversion script

v0.9.5 (2018-11-11)

- If the start position is greater than the end position, switch them around

v0.9.0 (2018-05-07)

- Removed clodius aggregate bigwig

v0.8.0 (2018-05-06)

- Bug fixes
- GeoJSON aggregation
- Multivec tiles

v0.7.4 (2018-02-14)

- Greatly sped up bedfile aggregation and fixed the maximum per tile limiting

v0.7.3 (2018-01-31)

- Use random importance by default

v0.7.2 (2017-12-04)

- Populate the xEnd field

v0.7.1 (2017-12-04)

- Added the 'header' field to beddb tiles

v0.7.0 (2017-10-04)

- Replaced get_2d_tile with get_2d_tiles
- Replaced get_tile with get_tiles

v0.6.5 (2017-07-14)

- Added a delimiter option to bedfiles
- Fixed beddb uid decoding
- Adding decoding to slugid.nice() methods

v0.6.0-0.6.4 (2017-07-13)

- python3 support

v0.5.0

- Added clodius aggregate bedpe
- More extensive unit tests

v0.4.7

- Renamed tsv to bedlike

v0.4.6

- Bug fixes in tile-text-file (tsv tiling)

v0.4.3

- When extracting a certain chromosome, place features at the position of the
  chromosome
