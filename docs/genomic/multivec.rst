Multivec Files
--------------

Multivec files store arrays of arrays organized by chromosome. To aggregate this
data, we need an input file where chromsome is a separate dataset. Example:

.. code-block:: python

    f = h5py.File('/tmp/blah.h5', 'w')

    d = f.create_dataset('chr1', (10000,5), compression='gzip')
    d[:] = np.random.random((10000,5))
    f.close()

This can be aggregated to multiple resolutions using `clodius aggregate multivec`:

.. code-block:: bash

    clodius aggregate multivec \
        --chromsizes-filename ~/projects/negspy/negspy/data/hg38/chromInfo.txt \
        --starting-resolution 1000 \
        --row-infos-filename ~/Downloads/sampled_info.txt \
        my_file_genome_wide_hg38_v2.multivec

The `--chromsizes-filename` option lists the chromosomes that are in the input
file and their sizes.  The `--starting-resolution` option indicates that the
base resolution for the input data is 1000 base pairs.

Epilogos Data (multivec)
------------------------

Epilogos (https://epilogos.altiusinstitute.org/) show the distribution of chromatin states
over a set of experimental conditions (e.g. cell lines). The data consist of positions and
states::

    chr1    10000   10200   id:1,qcat:[ [-0.2833,15], [-0.04748,5], [-0.008465,7], [0,2], [0,3], [0,4], [0,6], [0,10], [0,11], [0,12], [0,13], [0,14], [0.0006647,1], [0.436,8], [1.921,9] ]
    chr1    10200   10400   id:2,qcat:[ [-0.2833,15], [-0.04748,5], [0,3], [0,4], [0,6], [0,7], [0,10], [0,11], [0,12], [0,13], [0,14], [0.0006647,1], [0.004089,2], [0.8141,8], [1.706,9] ]
    chr1    10400   10600   id:3,qcat:[ [-0.2588,15], [-0.04063,5], [0,2], [0,3], [0,4], [0,6], [0,7], [0,10], [0,11], [0,12], [0,13], [0,14], [0.0006647,1], [0.2881,8], [1.58,9] ]
    chr1    10600   10800   id:4,qcat:[ [-0.02619,15], [0,1], [0,2], [0,3], [0,4], [0,6], [0,7], [0,8], [0,10], [0,11], [0,12], [0,13], [0,14], [0.1077,5], [0.4857,9] ]

This can be aggregated into multivec format:

.. code-block:: bash

    clodius convert bedfile_to_multivec \
        hg38/all.KL.bed.gz \
        --assembly hg38 \
        --starting-resolution 200 \
        --row-infos-filename row_infos.txt \
        --num-rows 15 \
        --format epilogos

States Data (multivec)
----------------------

A bed file with categorical data, e.g from chromHMM. The data consist of positions and states for each segment in categorical data::

  chr1	0	10000	Quies
  chr1	10000	10400	FaireW
  chr1	10400	15800	Low
  chr1	15800	16000	Pol2
  chr1	16000	16400	Gen3'
  chr1	16400	16600	Elon
  chr1	16600	139000	Quies
  chr1	139000	139200	Ctcf

This can be aggregated to multivec format:

.. code-block:: bash

    clodius convert bedfile_to_multivec \
        hg38/all.KL.bed.gz \
        --assembly hg38 \
        --starting-resolution 200 \
        --row-infos-filename row_infos.txt \
        --num-rows 7 \
        --format states
        --row_infos-filename rows_info.txt

A rows_info.txt file is required in the parameter ``--row-infos-filename`` for this type of data. This file contains the name of the states in the bedfile. e.g. rows_infos.txt::

     Quies
     FaireW
     Low
     Pol2
     Gen3'
     Elon
     ctcf

The number of rows with the name of the states in the rows_info.txt file must match the number of states in the bedfile and that number should be stated in the ``--num-rows`` parameter.

The resulting output file can be ingested using ``higlass-manage``:

.. code-block:: bash

    higlass-manage.py ingest --filetype multivec --datatype multivec data.mv5


Other Data (multivec)
---------------------

Multivec files are datatype agnostic. For use with generic data, create a
`segments` file containing the length of each segment. A segment is an
arbitrary set of discontinuous blocks that the data is partitioned into. In the
case of genomics data, segments correspond to chromosomes. If the
data has no natural grouping, it can all be lumped into one "segment"
which is wide enough to accommodate all the data points. Below is an
example of a dataset grouped into two "segments".

.. code-block:: bash

    segment1    20000
    segment2    40000

Data will be displayed as if the segments were laid out end to end::

.. code-block:: bash

    |---------------|------------------------------|
         segment1               segment2

The individual datapoints should then be formatted as in the block below. Each
row in this file corresponds to a column in the displayed plot. Each ``value``
is one of sections of the stacked bar plot or matrix that is rendered by the
multivec plot.

.. code-block:: bash

    segment_name    start  end  value1  value2   value3
    segment1            0 10000      1       2        1
    segment2        20000 30000      1       1        1

.. code-block:: bash

             ______
            |______|                 ______
            |      |                |______|
            |______|                |______|
            |      |                |      |
    |---------------|------------------------------|
         segment1               segment2

This can be converted to a multivec file using the following command:

.. code-block:: bash

    clodius convert bedfile_to_multivec \
        data.tsv \
        --chromsizes-file segments.tsv \
        --starting-resolution 1

This command can also take the parameter ``--row-infos-filename rows.txt`` to
describe, in human readable text, each row (e.g. cell types). The passed
file should have as many rows as there are rows in the multivec matrix.

The resulting output file can be ingested using ``higlass-manage``:

.. code-block:: bash

    higlass-manage.py ingest --filetype multivec --datatype multivec data.mv5
