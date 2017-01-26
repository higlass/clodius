# Clodius <img src="https://travis-ci.org/hms-dbmi/clodius.svg?branch=develop"/>

## Installation (Tested on `Ubuntu 16.04 LTS`)

### Requirements
* [bedtools](http://bedtools.readthedocs.io/en/latest/content/installation.html#installing-stable-releases)

Install `clodius` by running:

```
pip install -r requirements.txt
python setup.py install
python setup.py build_ext --inplace
```

## Documentation

Clodius is a tool for breaking up large data sets into smaller tiles that can
subsequently be displayed using an appropriate viewer. 

You can provide either a JSON or a tsv (tab separatred value) file as
input.  If a tsv file is provided, the column names must be passed in as an
argument using the --column-names (-c) argument. Also required is a paramter
which specifies the columns that will contain the position of each element in
the data set. The input data can be n-dimensional. Given the assumed goal of
visualization, we expect most use cases to be limited to 1 or 2 dimensional
data sets.

### Input format

The input can be either a JSON (a list of objects) or tsv (tab separated value)
files. TSV values need to be passed in with column headers (using the
`--column-names` or `-c` options). Eventually you should be able to pass in a
filename which contains the column names in its contents.

#### BedGraph files

Input can be in BedGraph format, but it needs to be preprocessed to work with
clodius. See the example below:

```
python scripts/process_file.py /data/encode/hg19/E002-H3K27me3.fc.signal.bigwig.bedGraph --type bedgraph --assembly hg19
```

This will create another file in the same directory as the original:

```
[ubuntu@ip hg19]$ lt ~/data/encode/hg19/E002-H3K27me3.fc.signal.bigwig.bedGraph.genome.sorted.gz
-rw-rw-r-- 1 ubuntu ubuntu 247M Sep  2 15:15 /home/ubuntu/data/encode/hg19/E002-H3K27me3.fc.signal.bigwig.bedGraph.genome.sorted.gz
```

This creates an absolute genome position bed graph (agped graph) file:

```
0       10128   0
10128   10159   0.41308
10159   10195   0.82616
```

#### BigWig files

BigWig files can be tiled using the bigWig format tile script (should take about 10 minutes for hg19):

```
python scripts/tile_bigWig.py ~/Downloads/wgEncodeCaltechRnaSeqHuvecR1x75dTh1014IlnaPlusSignalRep2.bigWig --output-file ~/Downloads/wgEncodeCaltechRnaSeqHuvecR1x75dTh1014IlnaPlusSignalRep2.hitile
```

An example bigWig file can be downloaded from the [ENCODE project](http://egg2.wustl.edu/roadmap/data/byFileType/signal/consolidated/macs2signal/foldChange/E044-H3K27me3.fc.signal.bigwig).

#### Create Tiles from an Absolute Genome Position Bed Graph file

Absolute genome position BedGraph files can be turned into 1D files using
clodius using the command below.  The environment variables are just there to
make it easy to run the same command using different datasets.

```
AWS_ES_DOMAIN=127.0.0.1:9200
ASSEMBLY=hg19
DATASET_NAME=E002-H3K27me3.fc.signal.bigwig.bedGraph.genome.sorted.gz
INDEX_NAME=hg19.1/${DATASET_NAME}
FILEPATH=/data/encode/${ASSEMBLY}/${DATASET_NAME}

zcat ${FILEPATH} | /usr/bin/time python scripts/make_single_threaded_tiles.py --assembly hg19 -b 256 -r 1 --expand-range 1,2 --ignore-0 -k 1 -v 3 --elasticsearch-url ${AWS_ES_DOMAIN}/${INDEX_NAME}
```

The option `--expand-range` indicates that the values in columns 1 and 2
indicate a range and that the value in column 3 (`--v 3`) represents all
positions in between. `--ignore-0` means that rows containing a value of 0
should be ignored.

### Output format

#### Files

Clodius can output a directory tree filled with tile files. The root of the
directory is specified using the (`--output-dir` or `-o` parameter). This
contains the `tile_info.json` file which contains all of the metadata about the
tiling:

```json
{
  "min_importance": 1.0, 
  "min_pos": [
    1.0, 
    1.0
  ], 
  "max_value": 1.0, 
  "min_value": 1.0, 
  "max_zoom": 1, 
  "max_width": 3.0, 
  "max_importance": 1.0, 
  "max_pos": [
    4.0, 
    4.0
  ]
}
```

Tiles are identified using an ID in the format `z.x.y` (e.g. `13.165.1765`). 
The first number (`z`) corresponds to the zoom level (0 or 1), the second (`x`
to the tile number along the first dimension (0, 1, or 2), and the third (`y`)
to the tile number along the third dimension (0, 1, or 2). If the input data
was 1D, then the id would have one less number.

##### Tile boundaries

Tile boundaries are half-open. This an be illustrated using the following
code block:

```python
    import json
    import clodius.fpark as cfp
    import clodius.tiles as cti

    sc = cfp.FakeSparkContext
    entries = [{'x1': 1, 'value': 5}, {'x1': 10, 'value': 6}]
    entries = cfp.FakeSparkContext.parallelize(entries)

    tileset = cti.make_tiles_by_importance(sc, entries, ['x1'], max_zoom=0, 
                                           importance_field='value', adapt_zoom=False)

    tiles = tileset['tiles'].collect()
    print json.dumps(tiles, indent=2)
```

The output will be:

```json
[
  [
    (0,1),
    {
      "end_pos": [
        10.0
      ],
      "x1": 10,
      "pos": [
        10.0
      ],
      "value": 6
    }
  ],
  [
    (0,0),
    {
      "end_pos": [
        1.0
      ],
      "x1": 1,
      "pos": [
        1.0
      ],
      "value": 5
    }
  ]
]
```

Notice that even though `max_zoom` is equal to 0, two tiles are generated. This is because
the width of the lowest resolution tile (`zoom_level=0`) is equal to the width of the domain
of the data. Because tile inclusion intervals are half-open, however, the second point will
actually be in a second tile `[0,1]`.

#### Saving Tiles

##### Compression

The output tiles can be gzipped by using the `--gzip` option. This will
compress each tile file and append `.gz` to its filename. Using this options is
generally recommended whenever tiles contain repetitive data and storage is at
a premium.

#### Matrix data

Gridded (matrix) data can be output in two forms: **sparse** and **dense**
format. Sparse format is useful for representing matrices where few of values
have non-zero values. In this format, we specify the location of each cell
that has a value along with its value.

##### Sparse Format

Used when there are not enough entries to make it worthwhile to return dense
matrix format. 

```json
{
  "sparse": [
    {
      "value": 1.0,
      "pos": [
        2.5,
        2.5
      ],
    }
  ],
}
```

The `sparse` field contains the actual data points that need to be displayed in
for this tile. At lower zoom levels, densely packed data needs to be abstracted
so that only a handful of data points are shown. This can be done by binning
and aggregating by summing or simply picking the most "important" points in the
area that this tile occupies.

##### Dense Format

The output of a single tile is just an array of values under the key `dense`.

```json
{
    "dense": [3.0, 0, 2.0, 1.0]
}
```

The encoding from bin positions, to positions in the flat array is performed thusly:

```python
for (bin_pos, bin_val) in tile_entries_iterator.items():
    index = sum([bp * bins_per_dimension ** i for i,bp in enumerate(bin_pos)])
    initial_values[index] = bin_val
```

The translation from `index` to `bin_pos` is left as en exercise to the reader.

#### Elasticsearch

Tiles can also be saved directly to an elasticsearch database using the `--elasticsearch-nodes`
and `--elasticsearch-path` options. Example

```
--elasticsearch-nodes localhost:9200 --elasticsearch-path test/tiles
```

When using Elasticsearch to serve tiles, make sure that cross-origin requests are allowed.
This is done by adding the following two lines to your `config/elasticsearch.yml` file:

```
http.cors.enabled: true
http.cors.allow-origin: '*'
```

## Usage Example

The directory tree and example files shown above can be generated using the
following command:

```
OUTPUT_DIR=output; rm -rf $OUTPUT_DIR/*; python scripts/make_tiles.py -o $OUTPUT_DIR \
-v count -p pos1,pos2 -c pos1,pos2,count -i count -b 2 --max-zoom 1 \
test/data/smallFullMatrix.tsv
```

Or, for a more realistic data set:

```
OUTPUT_DIR=output; rm -rf $OUTPUT_DIR; python scripts/make_tiles.py -o $OUTPUT_DIR -v count -p pos1,pos2 -c pos1,pos2,count -i count -r 5000 -b 128 --max-zoom 14 data/128k.raw; du --max-depth=0 -h output; find output/ | wc -l;
```

The parameters:


## Tests

There are a limited number of tests provided.

```
nosetests -s test/ 
```

## Examples




## Profiling

To see where this script spends the majority of the time, profile it using:

```
OUTPUT_DIR=output; rm -rf $OUTPUT_DIR/*; ./scripts/pyprof.sh scripts/make_tiles.py -o $OUTPUT_DIR -v count -p pos1,pos2 -c pos1,pos2,count -i count -b 512 --max-zoom 8 data/32k.raw
```

Note that to run this, you need to have
[`gprof2dot`](https://github.com/jrfonseca/gprof2dot) installed.

## Back of the envelope

Human genome: ~4 billion = 2 ^ 32 base pairs

One tile (at highest possible resolution): 256 = 2 ^ 8 base pairs

We would need to allow a zoom level of 24 to display the genome at a resolution of 1 bp / pixel.

To display it at 1K (~2 ^ 10) base pairs per pixel, we would need 14 zoom levels

## Testing

```
python setup.py build_ext --inplace
nosetests test
```

## Push new changes to pypi

```
bumpversion patch
python setup.py sdist upload -r pypi
```

