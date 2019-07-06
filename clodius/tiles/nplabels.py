import numpy as np
import clodius.tiles.npvector as ctn


def tiles(labels, z, x, importances=None, tile_size=16):
    '''
    Return tiles from the array. If importances are provided,
    then they will be used to prioritize entries. Otherwise we'll
    assign random importances to the entire array.
    '''
    max_zoom, x_start, x_end = ctn.max_zoom_and_data_bounds(
        labels, z, x, tile_size)

    tile_labels = np.asarray(labels[x_start:x_end])
    if tile_labels.dtype.kind == 'S':
        tile_labels = tile_labels.astype('U')

    if importances is not None and len(importances):
        tile_importances = np.asarray(importances[x_start:x_end])
        indices = np.argsort(tile_importances)[::-1][:tile_size]
    else:
        tile_importances = np.zeros(x_end - x_start)
        indices = np.linspace(x_start, x_end - 1,
                              tile_size, dtype=int) - x_start

    return [{'x': x, 'label': label, 'importance': importance}
            for x, label, importance
            in zip((x_start + indices).tolist(),
                   tile_labels[indices].tolist(),
                   tile_importances[indices].tolist())
            ]


def tiles_wrapper(array, tile_ids, importances):
    tile_values = []

    for tile_id in tile_ids:
        parts = tile_id.split('.')

        if len(parts) < 3:
            raise IndexError("Not enough tile info present")

        z = int(parts[1])
        x = int(parts[2])

        ret_array = tiles(array, z, x, importances)

        tile_values += [(tile_id, ret_array)]

    return tile_values
