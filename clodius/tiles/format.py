import base64
import numpy as np


def format_dense_tile(data):
    """
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
    """

    tile_data = {}

    if len(data):
        with np.warnings.catch_warnings():
            np.warnings.filterwarnings("ignore", r"All-NaN (slice|axis) encountered")

            max_dense = float(np.nanmax(data))
            min_dense = float(np.nanmin(data))
    else:
        max_dense = np.nan
        min_dense = np.nan

    tile_data["min_value"] = min_dense if not np.isnan(min_dense) else "NaN"
    tile_data["max_value"] = max_dense if not np.isnan(max_dense) else "NaN"

    min_f16 = np.finfo("float16").min
    max_f16 = np.finfo("float16").max

    has_nan = np.sum(np.isnan(data)) > 0
    n_dim = len(data.shape)
    size = data.shape[1] if n_dim == 2 else 1

    # Flatten data array
    fdata = data
    if n_dim > 0:
        fdata = data.flatten()

    if (
        not has_nan
        and max_dense > min_f16
        and max_dense < max_f16
        and min_dense > min_f16
        and min_dense < max_f16
    ):
        tile_data.update(
            {
                "dense": base64.b64encode(fdata.astype("float16")).decode("utf-8"),
                "size": size,
                "dtype": "float16",
            }
        )
    else:
        tile_data.update(
            {
                "dense": base64.b64encode(fdata.astype("float32")).decode("utf-8"),
                "size": size,
                "dtype": "float32",
            }
        )

    return tile_data
