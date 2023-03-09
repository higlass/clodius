### Converting tsv files to mrmatrix

```
tsv_to_mrmatrix data/dense.tsv data/dense.mrmatrix
```

### Calculating gene coverage

```
bedtools makewindows -g ~/projects/negspy/negspy/data/hg19/chromInfo.txt -w 1000 > ~/data/genbank-data/hg19/bins_hg19_1000.bed

awk '{ if ($6 == "-") print }' ~/data/genbank-data/hg19/geneAnnotationsExonUnions.bed > ~/data/genbank-data/hg19/geneAnnotationsExonUnions.-.bed
awk '{ if ($6 == "+") print }' ~/data/genbank-data/hg19/geneAnnotationsExonUnions.bed > ~/data/genbank-data/hg19/geneAnnotationsExonUnions.+.bed

bedtools coverage -b ~/data/genbank-data/hg19/geneAnnotationsExonUnions.+.bed -a ~/data/genbank-data/hg19/bins_hg19_1000.bed > ~/data/genbank-data/hg19/annotationsCoverage.+.bed
bedtools coverage -b ~/data/genbank-data/hg19/geneAnnotationsExonUnions.-.bed -a ~/data/genbank-data/hg19/bins_hg19_1000.bed > ~/data/genbank-data/hg19/annotationsCoverage.-.bed

clodius convert bedfile_to_multivec ~/data/genbank-data/hg19/annotationsCoverage.+.bed --assembly hg19 --starting-resolution 1000
clodius convert bedfile_to_multivec ~/data/genbank-data/hg19/annotationsCoverage.-.bed --assembly hg19 --starting-resolution 1000
```

### Converting bedfile to multivec

```
clodius convert bedfile_to_multivec test/sample_data/sample.bed.gz  --has-header --assembly hg38 --base-resolution 10
clodius aggregate multivec test/sample_data/sample.bed.multivec --assembly hg38 --base-resolution 10
```

### Getting gene summaries

```
cat ~/data/genbank-data/mm9/gene_summaries_*.tsv > ~/data/genbank-data/mm9/all_gene_summaries.tsv
```

### Building the documentation

```
make publish  # upload to PyPI

sphinx-build -b html docs docs/_build/html/

for file in $(ls ~/projects/francesco-tad-calls/TADcalls/Rao/GM12878/*.txt); do clodius aggregate bedpe --assembly hg19 --chr1-col 1 --from1-col 2 --to1-col 3 --chr2-col 1 --from2-col 2 --to2-col 3 $file; done;

for file in $(ls ~/projects/francesco-tad-calls/TADcalls/Rao/IMR90/*.txt); do clodius aggregate bedpe --assembly hg19 --chr1-col 1 --from1-col 2 --to1-col 3 --chr2-col 1 --from2-col 2 --to2-col 3 $file; done;
```

### Multivec files

```
cat ~/Dropbox/paper-data/Danielles-data/drosophila.chromSizes.orig | awk '{print $1 "\t" $NF}'  > ~/Dropbox/paper-data/Danielles-data/drosophila.chromSizes
clodius aggregate multivec --output-file ~/Dropbox/paper-data/Danielles-data/drosophila.multivec.hdf5 --chromsizes-filename ~/Dropbox/paper-data/Danielles-data/drosophila.chromSizes ~/Dropbox/paper-data/Danielles-data/drosophila.hdf5
```
