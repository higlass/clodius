Bedpe-like Files
----------------

BEDPE-like files contain two sets of chromosomal coordinates:

.. code-block:: bash

    chr10   74160000        74720000    chr10   74165000    74725000
    chr12   120920000       121640000   chr12   120925000   121645000
    chr15   86360000        88840000    chr15   86365000    88845000

To view such files in HiGlass, they have to be aggregated so that tiles don't
contain too many values and slow down the renderer:

.. code-block:: bash

    clodius aggregate bedpe \
        --assembly hg19 \
        --chr1-col 1 --from1-col 2 --to1-col 3 \
        --chr2-col 4 --from2-col 5 --to2-col 6 \
        --output-file domains.txt.multires \
        domains.txt

This requires the `--chr1-col`, `--from1-col`, `--to1-col`, `--chr2-col`,
`--from2-col`, `--to2-col` parameters to specify which columns in the datafile
describe the x-extent and y-extent of the region.

The priority with which regions are included in lower resolution tiles is
specified by the `--impotance-column` parameter. This can either provide a
value, contain `random`, or if it's not specified, default to the size of the
region.
