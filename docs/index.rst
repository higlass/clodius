.. clodius documentation master file, created by
   sphinx-quickstart on Mon Jul  3 16:40:45 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Clodius: Big data aggregation for visualization
===============================================

.. toctree::
    :maxdepth: 2
    :caption: Contents:

.. contents:: :local:


Displaying large amounts of data often requires first turning it into
not-so-large amounts of data. Clodius is a program and library designed
to aggregate large datasets to make them easy to display at different
resolutions.

BedGraph files
--------------

Aggregation by addition
^^^^^^^^^^^^^^^^^^^^^^^

Assume we have an input file that has ``id chr start end value1 value2`` pairs::

    1:2900001-3000000       1       2900001 3000000 -0.614  -0.495
    1:3000001-3100000       1       3000001 3100000 -0.407  -0.495
    1:3100001-3200000       1       3100001 3200000 -0.428  -0.495
    1:3200001-3300000       1       3200001 3300000 -0.437  -0.495

We can aggregate this file by recursively summing adjacent values. We have to 
indicate which column corresponds to the chromosome (``--chrom-col 2``), the
start position (``--from-pos-col 3``), the end position (``--to-pos-col 4``) and 
the value column (``--value-col 5``).

.. code-block:: bash

    clodius aggregate bedgraph 
        input_file.bg       \
        --from-pos-col 3    \
        --to-pos-col 4      \
        --value-col 5       \
        --assembly grch37

Aggregation by average
^^^^^^^^^^^^^^^^^^^^^^

In certain cases, the aggregation should be done by averaging adjacent
rather than just summing them:


.. code-block:: bash

    clodius aggregate bedgraph 
        input_file.bg       \
        --from-pos-col 3    \
        --to-pos-col 4      \
        --value-col 5       \
        --assembly grch37   \
        --method average


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
