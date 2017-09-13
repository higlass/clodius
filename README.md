# Clodius <img src="https://travis-ci.org/hms-dbmi/clodius.svg?branch=develop"/>

## Quick start without docker

Install the clodius package:

```
pip install clodius
```

And use it aggregate a bigWig file:

```
clodius aggregate bigwig ~/Downloads/E116-DNase.fc.signal.bigwig
```

The output files can then be displayed using the [higlass-docker container](https://github.com/hms-dbmi/higlass-docker). For more information about viewing these types of files take a look at the [higlass wiki](https://github.com/hms-dbmi/higlass/wiki#bigwig-files).

## Development


The recommended way to develop `clodius` is to use a [conda](https://conda.io/docs/intro.html) environment and
install `clodius` with develop mode:

```
python setup.py develop
```

Note that making changes to the `clodius/fast.pyx` [cython](http://docs.cython.org/en/latest/src/quickstart/cythonize.html) module requires an
explicit recompile step:

```
python setup.py build_ext --inplace
```

## Testing


The unit tests for clodius can be run using [nosetests](http://nose.readthedocs.io/en/latest/):

    nosetests tests

Individual unit tests can be specified by indicating the file and function
they are defined in:

```
nosetests test/cli_test.py:test_clodius_aggregate_bedgraph
```

## Quick start with Docker

If you don't have your own, get some sample data:
```
mkdir -p /tmp/clodius/input
mkdir -p /tmp/clodius/output
curl https://raw.githubusercontent.com/hms-dbmi/clodius/develop/test/sample_data/geneAnnotationsExonsUnions.short.bed \
  > /tmp/clodius/input/sample.short.bed 
```
Then install Docker, and pull and run the Clodius image:
```
docker stop clodius; 
docker rm clodius;

docker pull gehlenborglab/clodius # Ensure that you have the latest.

docker run -v /tmp/clodius/input:/tmp/ \
           gehlenborglab/clodius \
           clodius aggregate bigwig /tmp/file.bigwig
           
ls /tmp/clodius/input # Should contain the output file
```

If you already have a good location for your input and output files,
reference that in the `-v` arguments above, instead of `/tmp/clodius`.
The other scripts referenced below can be wrapped similarly.
