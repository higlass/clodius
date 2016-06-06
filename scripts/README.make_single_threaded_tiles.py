DATASET_NAME=hg19/Dixon2015-H1hESC_ES-HindIII-allreps-filtered.1kb.genome.gz.mirrored.shuffled
FILENAME=coolers/${DATASET_NAME}
FILEPATH=~/data/${FILENAME}

/usr/bin/time zcat ~/data/hg19/coolers/Dixon2015-H1hESC_ES-HindIII-allreps-filtered.1kb.genome.gz.10000000.sorted.gz  | head -n 1000000 | pypy scripts/make_single_threaded_tiles.py --min-pos 0,0 --max-pos 3137161264,3137161264 -b 256 -r 1000 --elasticsearch-url localhost:9200/${DATASET_NAME}

#####################
DATASET_NAME=hg19/E116-DNase.fc.signal.bigwig.bedGraph.genome.gz
FILEPATH=~/data/encode/${DATASET_NAME}

    zcat ${FILEPATH} | head -n 1000000 | pypy scripts/make_single_threaded_tiles.py --min-pos 1 --max-pos 3137161264 -b 64 -r 1 --expand-range 1,2 --ignore-0 -k 1 -v 3 --elasticsearch-url search-es4dn-z7rzz4kevtoyh5pfjkmjg5jsga.us-east-1.es.amazonaws.com/${DATASET_NAME}
zcat ${FILEPATH} | /usr/bin/time pypy scripts/make_single_threaded_tiles.py --min-pos 1 --max-pos 3137161264 -b 64 -r 1 --expand-range 1,2 --ignore-0 -k 1 -v 3 --elasticsearch-url search-es4dn-z7rzz4kevtoyh5pfjkmjg5jsga.us-east-1.es.amazonaws.com/${DATASET_NAME}


#####################
DATASET_NAME=hg19/wgEncodeSydhTfbsGm12878Ctcfsc15914c20StdSig.bigWig.bedGraph.genome.sorted.gz
FILEPATH=~/data/encode/${DATASET_NAME}

    zcat ${FILEPATH} | head -n 1000000 | pypy scripts/make_single_threaded_tiles.py --min-pos 1 --max-pos 3137161264 -b 64 -r 1 --expand-range 1,2 --ignore-0 -k 1 -v 3 --elasticsearch-url search-es4dn-z7rzz4kevtoyh5pfjkmjg5jsga.us-east-1.es.amazonaws.com/${DATASET_NAME}
zcat ${FILEPATH} | /usr/bin/time pypy scripts/make_single_threaded_tiles.py --min-pos 1 --max-pos 3137161264 -b 64 -r 1 --expand-range 1,2 --ignore-0 -k 1 -v 3 --elasticsearch-url search-es4dn-z7rzz4kevtoyh5pfjkmjg5jsga.us-east-1.es.amazonaws.com/${DATASET_NAME}

