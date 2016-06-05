DATASET_NAME=hg19/Dixon2015-H1hESC_ES-HindIII-allreps-filtered.1kb.genome.gz.mirrored.shuffled
FILENAME=coolers/${DATASET_NAME}
FILEPATH=~/data/${FILENAME}

/usr/bin/time zcat ~/data/hg19/coolers/Dixon2015-H1hESC_ES-HindIII-allreps-filtered.1kb.genome.gz.10000000.sorted.gz  | head -n 1000000 | pypy scripts/make_single_threaded_tiles.py --min-pos 0,0 --max-pos 3137161264,3137161264 -b 256 -r 1000 --elasticsearch-url localhost:9200/${DATASET_NAME}
