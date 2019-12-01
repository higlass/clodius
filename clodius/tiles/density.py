from clodius.tiles.format import format_dense_tile
from clodius.tiles.utils import tile_bounds
import pandas as pd

import numpy as np
import h5py


def csv_to_points(csv_file, output_file, max_zoom: int = 30):
    """
    Convert a csv file containing points to a numpy array
    of [[x,y]] values.

    Parameters:
    -----------
    csv_file: string
        The filename of the data file

    """
    df = pd.read_csv(csv_file)

    min_x = df["x"].min()
    max_x = df["x"].max()
    min_y = df["y"].min()
    max_y = df["y"].max()

    width = max_x - min_x
    height = max_y - min_y
    max_width = max(width, height)

    with h5py.File(output_file, "w") as f_out:
        dataset = f_out.create_dataset(
            "values", (len(df), 2), compression="gzip", dtype=np.float32
        )
        dataset[:] = df.reindex(columns=["x", "y"]).values

        dataset.attrs["min_x"] = min_x
        dataset.attrs["max_x"] = max_x
        dataset.attrs["min_y"] = min_y
        dataset.attrs["max_y"] = max_y
        dataset.attrs["max_zoom"] = max_zoom
        dataset.attrs["max_width"] = max_width

    return df.reindex(columns=["x", "y"])


def tileset_info(points_file):
    """
    Calculate the extent, etc...
    """
    with h5py.File(points_file, "r") as f_in:
        attrs = f_in["values"].attrs

        return {
            "min_pos": [float(attrs["min_x"]), float(attrs["min_y"])],
            "max_pos": [float(attrs["max_x"]), float(attrs["max_y"])],
            "max_width": float(attrs["max_width"]),
            "max_zoom": int(attrs["max_zoom"]),
            "mirror_tiles": "false",
        }


def filter_points(data, extent):
    """
    Filter points that are within the extent

    Parameters:
    -----------
    data: [[]]
        A 2D numpy array containing x,y values

    extent: [x_start, x_end, y_start, y_end]
        The region we want to return points within

    Returns
    -------
    data: [[]]
        A 2D numpy array containing x,y values
    """
    # print("extent:", extent)
    # print("data.shape", data.shape, data[:,0])
    data = data[data[:, 0] > extent[0]]
    data = data[data[:, 0] < extent[2]]

    data = data[data[:, 1] > extent[1]]
    data = data[data[:, 1] < extent[3]]

    return data


def density_tiles(points_file, z, x, y, width=1, height=1):
    """
    Get a 2D histogram of the given region. If the height and
    width are specified, then we need to partition this into
    multiple returned tiles.
    """
    returns = []

    with h5py.File(points_file, "r") as f:
        # get all the points in the region
        all_points = filter_points(
            f["values"][:],
            tile_bounds(tileset_info(points_file), z, x, y, width, height),
        )

        for i in range(width):
            for j in range(height):
                # filter from the larger subregion
                filtered_points = filter_points(
                    all_points, tile_bounds(tileset_info(points_file), z, x + i, y + j)
                )

                dt = np.histogram2d(
                    filtered_points[:, 0], filtered_points[:, 1], bins=256
                )[0].T
                dt[dt == 0.0] = np.nan

                returns += [((z, x + i, y + j), dt)]

        return returns


def tiles(points_file, z, x, y, width=1, height=1):
    return [
        (tile_pos, format_dense_tile(data.flatten()))
        for (tile_pos, data) in density_tiles(points_file, z, x, y, width, height)
    ]
