AWS_ES_DOMAIN=search-higlass-ssxwuix6kow3sekyeresi7ay5e.us-east-1.es.amazonaws.com



#################################

    DATASET_NAME=hg19/Rao2014-GM12878-MboI-allreps-filtered.1kb.cool.reduced.genome.5M.gz
    FILENAME=coolers/${DATASET_NAME}
    FILEPATH=~/data/${FILENAME}
    curl -XDELETE "${AWS_ES_DOMAIN}/${DATASET_NAME}"

    zcat ${FILEPATH} | /usr/bin/time pypy scripts/make_single_threaded_tiles.py --min-pos 0,0 --max-pos 5000000,5000000 -b 256 -r 1000 --elasticsearch-url ${AWS_ES_DOMAIN}/${DATASET_NAME}

    DATASET_NAME=hg19/Rao2014-GM12878-MboI-allreps-filtered.1kb.cool.reduced.genome.mmmili.gz
    FILENAME=coolers/${DATASET_NAME}
    FILEPATH=~/data/${FILENAME}
    curl -XDELETE "${AWS_ES_DOMAIN}/${DATASET_NAME}"

    zcat ${FILEPATH} | /usr/bin/time pypy scripts/make_single_threaded_tiles.py --min-pos 0,0 --max-pos 3137161264,3137161264 -b 256 -r 1000 --elasticsearch-url ${AWS_ES_DOMAIN}/${DATASET_NAME}

DATASET_NAME=hg19/Rao2014-GM12878-MboI-allreps-filtered.1kb.cool.reduced.genome.gz
FILENAME=coolers/${DATASET_NAME}
FILEPATH=~/data/${FILENAME}
curl -XDELETE "${AWS_ES_DOMAIN}/${DATASET_NAME}"
zcat ${FILEPATH} | /usr/bin/time pypy scripts/make_single_threaded_tiles.py --min-pos 0,0 --max-pos 3137161264,3137161264 -b 256 -r 1000 --elasticsearch-url ${AWS_ES_DOMAIN}/${DATASET_NAME} --num-threads 4 --triangular # 16:48:26

###############################
    DATASET_NAME=hg19/Dixon2015-H1hESC_ES-HindIII-allreps-filtered.1kb.cool.reduced.genome.5M.gz
    FILENAME=coolers/${DATASET_NAME}
    FILEPATH=~/data/${FILENAME}
    curl -XDELETE "${AWS_ES_DOMAIN}/${DATASET_NAME}"
    zcat ${FILEPATH} | /usr/bin/time pypy scripts/make_single_threaded_tiles.py --min-pos 0,0 --max-pos 5000000,5000000 -b 256 -r 1000 --elasticsearch-url ${AWS_ES_DOMAIN}/${DATASET_NAME} --triangular

    DATASET_NAME=hg19/Dixon2015-H1hESC_ES-HindIII-allreps-filtered.1kb.cool.reduced.genome.mmmili.gz
    FILENAME=coolers/${DATASET_NAME}
    FILEPATH=~/data/${FILENAME}
    curl -XDELETE "${AWS_ES_DOMAIN}/${DATASET_NAME}"

    zcat ${FILEPATH} | /usr/bin/time pypy scripts/make_single_threaded_tiles.py --min-pos 0,0 --max-pos 3137161264,3137161264 -b 256 -r 1000 --elasticsearch-url ${AWS_ES_DOMAIN}/${DATASET_NAME} --triangular

DATASET_NAME=hg19/Dixon2015-H1hESC_ES-HindIII-allreps-filtered.1kb.cool.reduced.genome.gz
FILENAME=coolers/${DATASET_NAME}
FILEPATH=~/data/${FILENAME}
curl -XDELETE "${AWS_ES_DOMAIN}/${DATASET_NAME}"
zcat ${FILEPATH} | /usr/bin/time pypy scripts/make_single_threaded_tiles.py --min-pos 0,0 --max-pos 3137161264,3137161264 -b 256 -r 1000 --elasticsearch-url ${AWS_ES_DOMAIN}/${DATASET_NAME} --num-threads 4 --triangular

#####################
DATASET_NAME=hg19/E116-DNase.fc.signal.bigwig.bedGraph.genome.sorted.gz
FILEPATH=~/data/encode/${DATASET_NAME}
curl -XDELETE "${AWS_ES_DOMAIN}/${DATASET_NAME}"

    zcat ${FILEPATH} | head -n 300000 | pypy scripts/make_single_threaded_tiles.py --min-pos 1 --max-pos 3137161264 -b 64 -r 1 --expand-range 1,2 --ignore-0 -k 1 -v 3 --elasticsearch-url ${AWS_ES_DOMAIN}/${DATASET_NAME}.5M

#zcat ${FILEPATH} | /usr/bin/time pypy scripts/make_single_threaded_tiles.py --min-pos 1 --max-pos 3137161264 -b 64 -r 1 --expand-range 1,2 --ignore-0 -k 1 -v 3 --elasticsearch-url ${AWS_ES_DOMAIN}/${DATASET_NAME}


#####################
DATASET_NAME=hg19/wgEncodeSydhTfbsGm12878Ctcfsc15914c20StdSig.bigWig.bedGraph.genome.sorted.gz
FILEPATH=~/data/encode/${DATASET_NAME}

    zcat ${FILEPATH} | head -n 100000 | pypy scripts/make_single_threaded_tiles.py --min-pos 1 --max-pos 3137161264 -b 64 -r 1 --expand-range 1,2 --ignore-0 -k 1 -v 3 --elasticsearch-url ${AWS_ES_DOMAIN}/${DATASET_NAME}.5M

#zcat ${FILEPATH} | /usr/bin/time pypy scripts/make_single_threaded_tiles.py --min-pos 1 --max-pos 3137161264 -b 64 -r 1 --expand-range 1,2 --ignore-0 -k 1 -v 3 --elasticsearch-url ${AWS_ES_DOMAIN}/${DATASET_NAME}

#####################
DATASET_NAME=hg19/wgEncodeSydhTfbsGm12878Pol2s2IggmusSig.bigWig.bedGraph.genome.sorted.gz
FILEPATH=~/data/encode/${DATASET_NAME}

    zcat ${FILEPATH} | head -n 100000 | pypy scripts/make_single_threaded_tiles.py --min-pos 1 --max-pos 3137161264 -b 64 -r 1 --expand-range 1,2 --ignore-0 -k 1 -v 3 --elasticsearch-url ${AWS_ES_DOMAIN}/${DATASET_NAME}

#zcat ${FILEPATH} | /usr/bin/time pypy scripts/make_single_threaded_tiles.py --min-pos 1 --max-pos 3137161264 -b 64 -r 1 --expand-range 1,2 --ignore-0 -k 1 -v 3 --elasticsearch-url ${AWS_ES_DOMAIN}/${DATASET_NAME}
#####################
DATASET_NAME=hg19/wgEncodeSydhTfbsGm12878InputStdSig.bigWig.bedGraph.genome.sorted.gz
FILEPATH=~/data/encode/${DATASET_NAME}
curl -XDELETE "${AWS_ES_DOMAIN}/${DATASET_NAME}"

    #zcat ${FILEPATH} | tail -n 100 >  /tmp/x
    #cat /tmp/x
    #cat /tmp/x | pypy scripts/make_single_threaded_tiles.py --min-pos 1 --max-pos 3137161264 -b 64 -r 1 --expand-range 1,2 --ignore-0 -k 1 -v 3 --elasticsearch-url ${AWS_ES_DOMAIN}/${DATASET_NAME}

zcat ${FILEPATH} | /usr/bin/time pypy scripts/make_single_threaded_tiles.py --min-pos 1 --max-pos 3137161264 -b 64 -r 1 --expand-range 1,2 --ignore-0 -k 1 -v 3 --elasticsearch-url ${AWS_ES_DOMAIN}/${DATASET_NAME}

#####################
DATASET_NAME=hg19/wgEncodeSydhTfbsGm12878Rad21IggrabSig.bigWig.bedGraph.genome.sorted.gz
FILEPATH=~/data/encode/${DATASET_NAME}
curl -XDELETE "${AWS_ES_DOMAIN}/${DATASET_NAME}"

    zcat ${FILEPATH} | head -n 10000 | pypy scripts/make_single_threaded_tiles.py --min-pos 1 --max-pos 3137161264 -b 64 -r 1 --expand-range 1,2 --ignore-0 -k 1 -v 3 --elasticsearch-url ${AWS_ES_DOMAIN}/${DATASET_NAME}

zcat ${FILEPATH} | /usr/bin/time pypy scripts/make_single_threaded_tiles.py --min-pos 1 --max-pos 3137161264 -b 64 -r 1 --expand-range 1,2 --ignore-0 -k 1 -v 3 --elasticsearch-url search-higlass-ssxwuix6kow3sekyeresi7ay5e.us-east-1.es.amazonaws.com/${DATASET_NAME}        # 7:42:23

#####################
DATASET_NAME=hg19/wgEncodeCrgMapabilityAlign36mer.bw.genome.sorted.gz
FILEPATH=~/data/encode/${DATASET_NAME}
curl -XDELETE "${AWS_ES_DOMAIN}/${DATASET_NAME}"

    #zcat ${FILEPATH} | head -n 1000 | pypy scripts/make_single_threaded_tiles.py --min-pos 1 --max-pos 3137161264 -b 64 -r 1 --expand-range 1,2 --ignore-0 -k 1 -v 3 --elasticsearch-url ${AWS_ES_INSTANCE}/${DATASET_NAME}

zcat ${FILEPATH} | /usr/bin/time pypy scripts/make_single_threaded_tiles.py --min-pos 1 --max-pos 3137161264 -b 64 -r 1 --expand-range 1,2 --ignore-0 -k 1 -v 3 --elasticsearch-url search-higlass-ssxwuix6kow3sekyeresi7ay5e.us-east-1.es.amazonaws.com/${DATASET_NAME}        # 5:52:04
