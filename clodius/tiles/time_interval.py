import json


def tileset_info(filename):
    '''
    Return a tileset info for this time interval
    '''
    with open(filename, 'r') as f:
        data = json.load(f)

        return {
            'min_pos': [0],
            'max_pos': [data['len']],
            'max_width': data['len'],
            'start_value': data['start'],
            'end_value': data['end']
        }
