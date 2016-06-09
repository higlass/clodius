import cStringIO as csio
import gzip
import json
import os
import os.path as op
import random
import requests
import sys
import time


class TileSaver(object):
    def __init__(self, max_data_in_sparse, bins_per_dimension, num_dimensions):
        self.max_data_in_sparse = max_data_in_sparse
        self.bins_per_dimension = bins_per_dimension
        self.num_dimensions = num_dimensions

        pass

    def save_tile(self, tile):
        return

    def make_and_save_tile(self, zoom_level, tile_position, tile_data):
        # this implementation shouldn't do anything
        # derived classes should implement this functionality themselves
        #print "saving:", tile_position

        #print "saving tile:", zoom_level, tile_position
        tile_id = "{}.{}".format(zoom_level, ".".join(map(str, tile_position)))
        tile = {'tile_id':  tile_id, "tile_value": tile_data}

        self.save_tile(tile)

    def save_dense_tile(self, zoom_level, tile_position, tile_bins, 
            min_value, max_value):
        initial_values = [0.0] * (self.bins_per_dimension ** self.num_dimensions)

        for (bin_pos, val) in tile_bins.items():
            index = sum([bp * self.bins_per_dimension ** i for i,bp in enumerate(bin_pos)])
            initial_values[index] = val

        self.make_and_save_tile(zoom_level, tile_position, {"dense": 
            [round(v, 5) for v in initial_values],
            'min_value': min_value, 'max_value': max_value })

    def save_sparse_tile(self, zoom_level, tile_position, tile_bins, 
            min_value, max_value):
        shown = []
        for (bin_pos, bin_val) in tile_bins.items():
            shown += [[bin_pos, bin_val]]

        self.make_and_save_tile(zoom_level, tile_position, {"sparse": shown,
            'min_value': min_value, 'max_value': max_value })

    def save_binned_tile(self, zoom_level, tile_position, tile_bins):
        max_value = max(tile_bins.values())
        min_value = min(tile_bins.values())

        #print "saving tile_position:", tile_position

        if len(tile_bins) < self.max_data_in_sparse:
            self.save_sparse_tile(zoom_level, tile_position, tile_bins, 
                                  min_value=min_value, max_value=max_value)
        else:
            self.save_dense_tile(zoom_level, tile_position, tile_bins,
                                 min_value=min_value, max_value=max_value)

    def flush():
        return

class EmptyTileSaver(TileSaver):
    def __init__(self, max_data_in_sparse, bins_per_dimension, num_dimensions):
        super(EmptyTileSaver, self).__init__(max_data_in_sparse, 
                                             bins_per_dimension,
                                             num_dimensions)


class ElasticSearchTileSaver(TileSaver):
    def __init__(self, max_data_in_sparse, bins_per_dimension, num_dimensions,
            es_path):
        super(ElasticSearchTileSaver, self).__init__(max_data_in_sparse, 
                                             bins_per_dimension,
                                             num_dimensions)
        self.es_path = es_path
        self.bulk_txt = csio.StringIO()
        self.bulk_txt_len = 0

        print "created tilesaver:", self.bulk_txt

    def save_tile(self, val):
        # this implementation shouldn't do anything
        # derived classes should implement this functionality themselves

        #self.bulk_txt.write(json.dumps({"index": {"_id": val['tile_id']}}) + "\n")

        self.bulk_txt.write('{{"index": {{"_id": "{}"}}}}\n'.format(val['tile_id']))
        self.bulk_txt.write(json.dumps(val) + "\n")

        '''
        self.bulk_txt.write('{{"tile_id": {}, "tile_value": '.format(val['tile_id']))

        if 'sparse' in val['tile_value']:
            self.bulk_txt.write(' {{ "sparse": [ ')
            self.bulk_txt.write(','.join(
                ['{{ "pos": [{}], "value": {} }}'.format(
                    ",".join([str(y) for y in x['pos']]), x['value'])
                    for x in val['tile_value']['sparse']]))
            self.bulk_txt.write('] }}')


        #print "val:", val
        #sys.exit(1)
        #new_string += str(val) + "\n"

        #self.bulk_txt_len += len(new_string)
        '''

        curr_pos = self.bulk_txt.tell()
        #print "curr_pos:", curr_pos,self.bulk_txt.getvalue()
        #self.bulk_txt.write(new_string)
        if curr_pos > 200000:
            self.flush()

    def flush(self):
        if self.bulk_txt.tell() > 0:
            # only save the tile if it had enough data
            save_to_elasticsearch("http://" + self.es_path + "/_bulk", self.bulk_txt.getvalue())

            self.bulk_txt_len = 0
            self.bulk_txt.close()
            self.bulk_txt = csio.StringIO()

def save_tile_to_elasticsearch(partition, elasticsearch_nodes, elasticsearch_path):
    bulk_txt = ""
    es_url = op.join(elasticsearch_nodes, elasticsearch_path)
    put_url =  op.join(es_url, "_bulk")

    for val in partition:
        bulk_txt += json.dumps({"index": {"_id": val['tile_id']}}) + "\n"
        bulk_txt += json.dumps(val) + "\n"

        if len(bulk_txt) > 2000000:
            save_to_elasticsearch("http://" + put_url, bulk_txt)
            bulk_txt = ""

    #print "len(bulk_txt)", len(bulk_txt)
    if len(bulk_txt) > 0:
        save_to_elasticsearch("http://" + put_url, bulk_txt)

def save_to_elasticsearch(url, data):
    '''
    Save some data to elastic search.

    The data should be a string suitable for bulk import by
    elasticsearch. The url should be the location of the index, document
    type, along with the _bulk destination.

    :param url: The elasticsearch url that will ingest the data
                e.g. localhost:9200/hg19/tiles/_bulk
    :param data: The data to import.
                e.g. {"index": {"_id": "blah", "my_json": {"x": 2}}}
    '''
    saved = False
    to_sleep = 1
    while not saved:
        try:
            #print "Saving... url:", url, "len(bulk_txt):", len(data)
            #print "data:", data
            r = requests.post(url, data=data, timeout=8)
            #print "data:", data
            print "Saved", r, "len(data):", len(data), url #, r.text
            saved = True
            #print "data:", data
        except Exception as ex:

            to_sleep *= 2
            print >>sys.stderr, "Error saving to elastic search, sleeping:", to_sleep, ex
            time.sleep(to_sleep)

            if to_sleep > 600:
                print >>sys.stderr, "Slept too long, returning"
                return

def save_tile(tile, output_dir, gzip_output):
    '''
    Save a tile to a particular base directory.

    This function create the appropriate sub-directory based on the
    key. 

    They key should be in the format (zoom_level, pos1, pos2...)
    e.g. (5,4,5)
    '''
    key = tile[0]
    tile_value = tile[1]

    outpath = op.join(output_dir, '.'.join(map(str, key)))
    outdir = op.dirname(outpath)

    if not op.exists(outdir):
        try:
            os.makedirs(outdir)
        except OSError as oe:
            # somebody probably made the directory in between when we
            # checked if it exists and when we're making it
            print >>sys.stderr, "Error making directories:", oe

    output_json = {"_source": {"tile_id": ".".join(map(str, key)),
                               "tile_value": tile_value}}
    if gzip_output:
        with gzip.open(outpath + ".gz", 'w') as f:
            f.write(json.dumps(output_json, indent=2))
    else:
        with open(outpath, 'w') as f:
            f.write(json.dumps(output_json, indent=2))

