Bed Files
---------

BED files specify genomic intervals. They are aggregated according to an
importance function that determines which values should be visible at lower
zoom levels. This importance function is user specified. In the absence of
any clear ranking of the different lines in the BED file, a random value
can be used in lieu of the importance function.

Example BED file:

.. code-block:: bash

    chr9    135766734       135820020       TSC1    Biallelic inactivation may predict sensitivity to MTOR inhibitors
    chr16   2097895 2138721 TSC2    Biallelic inactivation may predict sensitivity to MTOR inhibitors
    chr3    10183318        10195354        VHL     May signal the presence of a germline mutation.
    chr11   32409321        32457081        WT1     May signal the presence of a germline mutation.

This file can be aggregated like so:

.. code-block:: bash

    clodius aggregate bedfile \
        --assembly hg19 \
        short.bed

And then imported into higlass after copying to the docker temp directory (``cp short.bed.multires ~/hg-tmp/``):

.. code-block:: bash

     docker exec higlass-container python \
        higlass-server/manage.py ingest_tileset \
            --filename /tmp/short.bed.multires \
            --filetype beddb \
            --datatype bedlike \
            --coordSystem b37
