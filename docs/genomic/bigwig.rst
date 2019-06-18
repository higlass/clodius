bigWig files
------------

`bigWig files <https://genome.ucsc.edu/goldenpath/help/bigWig.html>`_ store
genomic data in a compressed, indexed form that allows rapid retrieval and
visualization. bigWig files can be loaded directly into HiGlass using the
vector datatype and bigwig filetype:

.. code-block:: bash

    docker exec higlass-container python \
            higlass-server/manage.py ingest_tileset \
            --filename /tmp/cnvs_hw.bigWig \
            --filetype bigwig \
            --datatype vector \
            --coordSystem hg19

**Important:** BigWig files have to be associated with a chromosome order!!
This means that there needs to be a chromsizes file for the
specified assembly (coordSystem) in the higlass database. If no ``coordSystem``
is specified for the bigWig file in ``ingest_tileset``, HiGlass will try to
find one in the database that matches the chromosomes present in the bigWig file.
If a ``chromsizes`` tileset is found, it's ``coordSystem`` will also be used for
the bigWig file. If none are found, the import will fail. If more than one is found,
the import will also fail. If a `coordSystem` is specified for the bigWig, but no
``chromsizes`` are found on the server, the import will fail.

TLDR: The simplest way to import a bigWig is to have a ``chromsizes`` present e.g.

| ``ingest_tileset --filetype chromsizes-tsv --datatype chromsizes --coordSystem hg19 --filename chromSizes.tsv``

and then to add the bigWig with the same ``coordSystem``:

| ``ingest_tileset --filetype bigwig --datatype vector --coordSystem hg19 --filename cnvs_hw.bigWig``
