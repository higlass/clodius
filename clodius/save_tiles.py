import gzip
import json
import os
import os.path as op
import requests

def save_tile_to_elasticsearch(partition, elasticsearch_nodes, elasticsearch_path):
    bulk_txt = ""
    es_url = op.join(elasticsearch_nodes, elasticsearch_path)
    put_url =  op.join(es_url, "_bulk")

    for val in partition:
        bulk_txt += json.dumps({"index": {"_id": val['tile_id']}}) + "\n"
        bulk_txt += json.dumps(val) + "\n"

        if len(bulk_txt) > 5000000:
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
            requests.post(url, data=data, timeout=8)
            #print "Saved"
            saved = True
        except Exception as ex:

            to_sleep *= 2
            #print >>sys.stderr, "Error saving to elastic search, sleeping:", to_sleep, ex
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

