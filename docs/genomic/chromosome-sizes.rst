Chromosome Sizes
----------------

Chromosome sizes can be used to create chromosome label and chromosome grid tracks.
They consist of a tab-separated file containing chromosome names and sizes
as columns:

.. code-block:: bash

    chr1    249250621
    chr2    243199373
    chr3    198022430
    ...

Chromosome sizes can be imported into the higlass server using the ``--filetype chromsizes-tsv`` and ``--datatype chromsizes`` parameters. A ``coordSystem`` should be included to identify the assembly that these chromosomes define.

| ``ingest_tileset --filetype chromsizes-tsv --datatype chromsizes --coordSystem hg19 chromSizes.tsv``
