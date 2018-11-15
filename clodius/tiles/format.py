import base64
import numpy as np

def format_dense_tile(data):
    '''
    Format raw vector data into a more structured tile
    containing either float16 or float32 data along with a 
    dtype to differentiate between the two.

    Parameters
    ----------
    tile_data_array: np.array
        An array of values
    Returns
    -------
    tile_data: {'dense': str, 'dtype': str}
        The tile data reformatted to use float16 or float32 as the
        datatype. The dtype indicates which format is chosen.
    '''

    tile_data = {}

    if len(data):
        with np.warnings.catch_warnings():
            np.warnings.filterwarnings('ignore', r'All-NaN (slice|axis) encountered')

            max_dense = float(np.nanmax(data))
            min_dense = float(np.nanmin(data))
    else:
        max_dense = np.nan
        min_dense = np.nan

    tile_data["min_value"] = min_dense if not np.isnan(min_dense) else "NaN"
    tile_data["max_value"] = max_dense if not np.isnan(max_dense) else "NaN"

    min_f16 = np.finfo('float16').min
    max_f16 = np.finfo('float16').max

    #has_nan = len([d for d in data if np.isnan(d)]) > 0
    has_nan = np.sum(np.isnan(data)) > 0

    if (
        not has_nan and
        max_dense > min_f16 and max_dense < max_f16 and
        min_dense > min_f16 and min_dense < max_f16
    ):
        tile_data.update({
            'dense': base64.b64encode(data.astype('float16')).decode('utf-8'),
            'dtype': 'float16'
        })
    else:
        tile_data.update({
            'dense': base64.b64encode(data.astype('float32')).decode('utf-8'),
            'dtype': 'float32'
        })

    return tile_data

