Hitile files
------------

Hitile files are HDF5-based 1D vector files containing data at multiple resolutions.

To see hitile datasets in higlass, use the docker container to load them:

.. code-block:: bash

    docker exec higlass-container python \
            higlass-server/manage.py ingest_tileset \
            --filename /tmp/cnvs_hw.hitile \
            --filetype hitile \
            --datatype vector

Point your browser at 127.0.0.1:8989 (or wherever it is hosted), click on the
little 'plus' icon in the view and select the top position.  You will see a
listing of available tracks that can be loaded. Select the dataset and then
choose the plot type to display it as.
