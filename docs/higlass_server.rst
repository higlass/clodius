HiGlass Server
==============

.. toctree::
    :maxdepth: 2
    :caption: Contents:

Development
-----------

Running the server locally:

.. code-block:: bash
    
    python manage.py runserver 8000

Importing data
--------------

Different types of data can be imported into the higlass server.


Chromosome sizes
~~~~~~~~~~~~~~~~

Chromosome sizes specify the lengths of the chromosomes that make up an
assembly. While they have no intrinsic biological order, HiGlass displays all
chromosomes together on a line so the order of the entries in the file does
have a meaning.

They must be imported with the `chromsizes-tsv` filetype and `chromsizes`
datatype to be properly recognized by the server and the API.

.. code-block:: bash

    docker exec higlass-container python \
            higlass-server/manage.py ingest_tileset \
            --filename /tmp/cnvs_hw.hitile \
            --filetype chromsizes-tsv \
            --datatype chromsizes

Testing
^^^^^^^

.. code-block:: bash

    python manage.py test tilesets --failfast

Or to test a more specific code block:

.. code-block:: bash

    python manage.py test tilesets.tests.CoolerTest.test_transforms --failfast
