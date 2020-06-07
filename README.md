# Clodius

[![Build Status](https://travis-ci.org/higlass/clodius.svg?branch=develop)](https://travis-ci.org/higlass/clodius)
[![Docs](https://img.shields.io/badge/docs-📖-red.svg?colorB=6680ff)](https://docs.higlass.io/data_preparation.html)

Displaying large amounts of data often requires first turning it into
not-so-large amounts of data. Clodius is a program and library designed
to aggregate large datasets to make them easy to display at different
resolutions.

## Demo

Install the clodius package:

```shell
pip install clodius
```

And use it aggregate a BED file:

```shell
curl https://raw.githubusercontent.com/hms-dbmi/clodius/develop/test/sample_data/geneAnnotationsExonsUnions.short.bed \
  > /tmp/sample.short.bed
clodius aggregate bedfile /tmp/sample.short.bed
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
  - [BigBed Files](docs/genomic/bigbed.rst)
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
pip install -e .
```
