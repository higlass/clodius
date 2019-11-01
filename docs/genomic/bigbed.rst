bigBed files
------------

`bigBed files <https://genome.ucsc.edu/goldenpath/help/bigBed.html>`_ store
genomic annotation data (particularly exons) in an optionally compressed, 
indexed form that allows rapid retrieval and visualization. bigBed files 
can be loaded directly into HiGlass using the ``gene-bed12-annotation`` 
datatype and ``bigbed`` filetype:

.. code-block:: bash

    docker exec higlass-container python \
            higlass-server/manage.py ingest_tileset \
            --filename /tmp/annotations.bigBed \
            --filetype bigbed \
            --datatype gene-bed12-annotation \
            --coordSystem hg38

**Important:** BigBed files have to be associated with a chromosome order!!
This means that there needs to be a chromsizes file for the
specified assembly (coordSystem) in the higlass database. If no ``coordSystem``
is specified for the bigBed file in ``ingest_tileset``, HiGlass will try to
find one in the database that matches the chromosomes present in the bigBed file.
If a ``chromsizes`` tileset is found, it's ``coordSystem`` will also be used for
the bigBed file. If none are found, the import will fail. If more than one is found,
the import will also fail. If a `coordSystem` is specified for the bigBed, but no
``chromsizes`` are found on the server, the import will fail.

TLDR: The simplest way to import a bigBed is to have a ``chromsizes`` present e.g.

| ``ingest_tileset --filetype chromsizes-tsv --datatype chromsizes --coordSystem hg38 --filename chromSizes.hg38.tsv``

and then to add the bigBed with the same ``coordSystem``:

| ``ingest_tileset --filetype bigbed --datatype gene-bed12-annotation --coordSystem hg38 --filename annotations.bigBed``
