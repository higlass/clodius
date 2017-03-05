# Clodius <img src="https://travis-ci.org/hms-dbmi/clodius.svg?branch=develop"/>

## Quick start without docker

Clone the clodius repository (preferably into a virtual environment) and install the requirements:

```
git clone https://github.com/hms-dbmi/clodius.git
pip install --upgrade -r requirements.txt

python scripts/conversion_wrapper.py    \
    -i ~/Downloads/Dixon2012-H1hESC-HindIII-allreps-filtered.1000kb.cool   \
    -o ~/Downloads/Dixon2012-H1hESC-HindIII-allreps-filtered.1000kb.multires.cool   \
    --filetype cooler \
    --assembly hg19
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
