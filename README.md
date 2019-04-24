# Clodius <img src="https://travis-ci.org/hms-dbmi/clodius.svg?branch=develop"/>

Displaying large amounts of data often requires first turning it into
not-so-large amounts of data. Clodius is a program and library designed
to aggregate large datasets to make them easy to display at different
resolutions.

## Demo

Install the clodius package:

```shell
pip install clodius
```

And use it aggregate a bigWig file:

```shell
clodius aggregate bigwig ~/Downloads/E116-DNase.fc.signal.bigwig
```

The output files can then be displayed using the [higlass-docker container](https://github.com/hms-dbmi/higlass-docker). For more information about viewing these types of files take a look at the [higlass wiki](https://github.com/hms-dbmi/higlass/wiki#bigwig-files).

[More examples](COMMANDS.md) are available.

## File Types

- Non-genomic Rasters
  - [TSV Files](docs/raster/tsv.rst)
- Genomic Data
  - [Bed Files](docs/genomic/bed.rst)
  - [BedGraph Files](docs/genomic/bedgraph.rst)
  - [Bedpe-like Files](docs/genomic/bedpe.rst)
  - [BigWig Files](docs/genomic/bigwig.rst)
  - [Chromosome Sizes](docs/genomic/chromosome-sizes.rst)
  - [Cooler Files](docs/genomic/cooler.rst)
  - [Gene Annotation](docs/genomic/gene-annotation.rst)
  - [HiTile Files](docs/genomic/hitile.rst)
  - [Multivec Files](docs/genomic/multivec.rst)

## Development


The recommended way to develop `clodius` is to use a [conda](https://conda.io/docs/intro.html) environment and
install `clodius` with develop mode:

```shell
python setup.py develop
```

Note that making changes to the `clodius/fast.pyx` [cython](http://docs.cython.org/en/latest/src/quickstart/cythonize.html) module requires an
explicit recompile step:

```shell
python setup.py build_ext --inplace
```

## Testing


The unit tests for clodius can be run using [nosetests](http://nose.readthedocs.io/en/latest/):

```shell
nosetests test
```

Individual unit tests can be specified by indicating the file and function
they are defined in:

```shell
nosetests test/cli_test.py:test_clodius_aggregate_bedgraph
```

## Quick start with Docker

If you don't have your own, get some sample data:
```shell
mkdir -p /tmp/clodius/input
mkdir -p /tmp/clodius/output
curl https://raw.githubusercontent.com/hms-dbmi/clodius/develop/test/sample_data/geneAnnotationsExonsUnions.short.bed \
  > /tmp/clodius/input/sample.short.bed
```
Then install Docker, and pull and run the Clodius image:
```shell
docker stop clodius;
docker rm clodius;

docker pull gehlenborglab/clodius # Ensure that you have the latest.

docker run -v /tmp/clodius/:/tmp/ \
           gehlenborglab/clodius \
           clodius aggregate bigwig /tmp/input/file.bigwig

ls /tmp/clodius/output # Should contain the output file
```

If you already have a good location for your input and output files,
reference that in the `-v` arguments above, instead of `/tmp/clodius`.
The other scripts referenced below can be wrapped similarly.
