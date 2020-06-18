Cooler files
------------
`Cooler files <https://github.com/mirnylab/cooler>`_ (extension .cool) store
arbitrarily large 2D genomic matrices, such as those produced via Hi-C and other high
throughput proximity ligation experiments. HiGlass can render cooler files containing
matrices of the same dataset at a range of bin resolutions or *zoom levels*, so called multiresolution
cool files (typically denoted .mcool).

From pairs
^^^^^^^^^^

.. note:: Starting with *cooler* 0.7.9, input pairs data no longer needs to be sorted and indexed.

Often you will start with a **list of pairs** (e.g. contacts, interactions) that need to be aggregated.
For example, the 4DN-DCIC developed a `standard pairs format <https://github.com/4dn-dcic/pairix/blob/master/pairs_format_specification.md>`_ for HiC-like data. In general, you
only need a tab-delimited file with columns representing ``chrom1``, ``pos1``, ``chrom2``, ``pos2``, optionally gzipped. In the case of Hi-C, these would correspond to the mapped locations of the two ends of a Hi-C ligation product.

You also need to provide a list of chromosomes in semantic order (chr1, chr2, ..., chrX, chrY, ...) in a
two-column `chromsizes <https://github.com/pkerpedjiev/negspy/blob/master/negspy/data/hg19/chromSizes.tsv>`_ file.

Ingesting pairs is done using the ``cooler cload`` command. Choose the appropriate loading subcommand. If you pairs file is sorted and indexed with `pairix <https://github.com/4dn-dcic/pairix>`_ or with `tabix <https://davetang.org/muse/2013/02/22/using-tabix/>`_, use ``cooler cload pairix`` or ``cooler cload tabix``, respectively. Otherwise, you can use the new ``cooler cload pairs`` command.

**Raw pairs example**

If you have a raw pairs file or you can stream your data in such a way, you only need to specify the columns that correspond to `chrom1`, `chrom2`, `pos1` and `pos2`. For example, if ``chrom1`` and ``pos1`` are the first two columns, and ``chrom2`` and ``pos2`` are in columns 4 and 5, the following command will aggregate the input pairs at 1kb:

.. code-block:: bash

    cooler cload pairs -c1 1 -p1 2 -c2 4 -p2 5 \
        hg19.chrom.sizes:1000 \
        mypairs.txt \
        mycooler.1000.cool

To pipe in a stream, replace the pairs path above with a dash ``-``.

.. note:: The syntax ``<chromsizes_path>:<binsize_in_bp>`` is a shortcut to specify the genomic bin segmentation used to aggregate the pairs. Alternatively, you can pass in the path to a 3-column BED file of bins.


**Indexed pairs example**

If you want to create a sorted and indexed pairs file, follow this example. Because an index provides random access to the pairs, this method can be more efficient and parallelized.

.. code-block:: bash

    cooler csort -c1 1 -p1 2 -c2 4 -p2 5 mypairs.txt hg19.chrom.sizes

will generate a sorted and compressed pairs file ``mypairs.blksrt.txt.gz`` along with a companion pairix ``.px2`` index file. To aggregate, use the ``cload pairix`` command.

.. code-block:: bash

    cooler cload pairix hg19.chrom.sizes:1000 mypairs.blksrt.txt.gz mycooler.1000.cool

The output ``mycooler.1000.cool`` will serve as the *base resolution* for the multires cooler you will generate.

From a matrix
^^^^^^^^^^^^^
If your base resolution data is **already aggregated**, you can ingest data in one of two formats. Use ``cooler load`` to ingest.

.. note:: Prior to *cooler* 0.7.9, input BG2 files needed to be sorted and indexed. This is no longer the case.

1. **COO**: Sparse matrix upper triangle `coordinate list <https://en.wikipedia.org/wiki/Sparse_matrix#Coordinate_list_(COO)>`_ , i.e. tab-delimited sparse matrix triples (``row_id``, ``col_id``, ``count``). This is an output of pipelines like HiCPro.

.. code-block:: bash

    cooler load -f coo hg19.chrom.sizes:1000 mymatrix.1kb.coo.txt mycooler.1000.cool

2. **BG2**: A 2D "extension" of the `bedGraph <https://genome.ucsc.edu/goldenpath/help/bedgraph.html>`_ format. Tab delimited with columns representing ``chrom1``, ``start1``, ``end1``, ``chrom2``, ``start2``, ``end2``, and ``count``.

.. code-block:: bash

    cooler load -f bg2 hg19.chrom.sizes:1000 mymatrix.1kb.bg2.gz mycooler.1000.cool

Zoomify
^^^^^^^
To recursively aggregate your matrix into a multires file, use the ``zoomify`` command.

.. code-block:: bash

    cooler zoomify mycooler.1000.cool

The output will be a file called ``mycooler.1000.mcool`` with zoom levels increasing by factors of 2. You can also
request an explicit list of resolutions, as long as they can be obtained via integer multiples starting from the base resolution. HiGlass performs well as long as zoom levels don't differ in resolution by greater than a factor of ~5.

.. code-block:: bash

    cooler zoomify -r 5000,10000,25000,50000,100000,500000,1000000 mycooler.1000.cool

If this is Hi-C data or similar, you probably want to apply iterative correction (i.e. matrix balancing normalization) by including the ``--balance`` option.

Loading pre-zoomed data
^^^^^^^^^^^^^^^^^^^^^^^
If the matrices for the resolutions you wish to visualize are already available, you can ingest each one independently into the right location inside the file using the `Cooler URI <http://cooler.readthedocs.io/en/latest/api.html#uri-string>`_ ``::`` syntax.

HiGlass expects each zoom level to be stored at a location named ``resolutions/{binsize}``.

.. code-block:: bash

    cooler load -f bg2 hg19.chrom.sizes:1000 mymatrix.1kb.bg2 mycooler.mcool::resolutions/1000
    cooler load -f bg2 hg19.chrom.sizes:5000 mymatrix.5kb.bg2 mycooler.mcool::resolutions/5000
    cooler load -f bg2 hg19.chrom.sizes:10000 mymatrix.10kb.bg2 mycooler.mcool::resolutions/10000
    ...

.. seealso:: See the *cooler* `docs <http://cooler.readthedocs.io/>`_ for more information.
    You can also type ``-h`` or ``--help`` after any cooler command for a detailed description.


.. _loading-into-higlass:
