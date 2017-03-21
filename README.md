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
docker pull gehlenborglab/clodius
docker run -v /tmp/clodius/input:/tmp/input \
           -v /tmp/clodius/output:/tmp/output \
           gehlenborglab/clodius \
           python scripts/conversion_wrapper.py \
               -i /tmp/input/sample.short.bed \
               -o /tmp/output/sample.multires.bed \
               --filetype gene_annotation \
               --assembly hg19
ls /tmp/clodius/output # Should contain the output file
```

If you already have a good location for your input and output files,
reference that in the `-v` arguments above, instead of `/tmp/clodius`.
The other scripts referenced below can be wrapped similarly.
