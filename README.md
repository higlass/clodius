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

### Output format

Clodius outputs a directory tree filled with tile files. The root of the
directory is specified using the (`--output-dir` or `-o` parameter). This
contains the `tile_info.json` file which contains all of the metadata about the
tiling:

```
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

The directory structure of of the output directory allows for quick access to
tiles using the path `z/x/y.json`.

```
├── 0
│   ├── 0
│   │   ├── 0.json
│   │   └── 1.json
│   └── 1
│       └── 1.json
├── 1
│   ├── 0
│   │   ├── 0.json
│   │   ├── 1.json
│   │   └── 2.json
│   ├── 1
│   │   ├── 1.json
│   │   └── 2.json
│   └── 2
│       └── 2.json
└── tile_info.json
```

The first level (`z`) corresponds to the zoom level (0 or 1), the second (`x`
to the tile number along the first dimension (0, 1, or 2), and the third (`y`)
to the tile number along the third dimension (0, 1, or 2). If the input data
was 1D, then the directory structure would have one less level.

Each tile looks like this:

```
{
  "shown": [
    {
      "count": 1.0,
      "pos": [
        2.5,
        2.5
      ],
      "uid": "LvntW7KwUPhsamwnuSH9qm"
    }
  ],
  "tile_start_pos": [
    2.5,
    2.5
  ],
  "tile_end_pos": [
    4.0,
    4.0
  ]
}
```

The `shown` field contains the actual data points that need to be displayed in
for this tile. At lower zoom levels, densely packed data needs to be abstracted
so that only a handful of data points are shown. This can be done by binning
and aggregating by summing or simply picking the most "important" points in the
area that this tile occupies.

## Usage Example

The directory tree and example files shown above can be generated using the
following command:

```
OUTPUT_DIR=output; rm -rf $OUTPUT_DIR/*; python scripts/make_tiles.py -o $OUTPUT_DIR \
-v count -p pos1,pos2 -c pos1,pos2,count -i count -b 2 --max-zoom 1 \
test/data/smallfullMatrix.tsv
```

The parameters:


## Tests

There are a limited number of tests provided.

```
nosetests -s test/ 
```

## Timing

To obtain benchmarks, run clodius using a variety of maximum zooms, bin_sizes
and input data sizes:

```
for max_zoom in 14; do
    for bin_size in 256; do
        for data_size in 16k 32k 64k 128k 256k 512k 1m 2m 4m 8m 16m 32m; do # 16k 32k 64k 128k 256k 512k 1m 2m 4m; do #1m 2m 4m; do
            OUTPUT_DIR=output;
            rsync -a --delete blank/ output/
            time=`/usr/bin/time -f "user: %E %M" /home/ubuntu/apps/spark-1.6.1-bin-hadoop2.6/bin/spark-submit scripts/make_tiles.py -o $OUTPUT_DIR -v count -p pos1,pos2 -c pos1,pos2,count -i count -r 5000 -b ${bin_size} --max-zoom $max_zoom --use-spark data/${data_size}.raw | grep user | awk '{ print $2 " " $3}'`;
            #time=`/usr/bin/time -f "user: %E %M" python scripts/make_tiles.py -o $OUTPUT_DIR -v count -p pos1,pos2 -c pos1,pos2,count -i count -r 5000 -b ${bin_size} --max-zoom $max_zoom data/${data_size}.raw 2>&1 | grep user | awk '{ print $2 " " $3}'`;
            num_files=`find output/ | wc -l`
            size=`du --max-depth=0 -h output | awk '{ print $1}'`
            echo $max_zoom $bin_size $num_files $data_size $time $size
        done;
    done;
done;
```

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

