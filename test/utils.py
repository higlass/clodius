import h5py
import logging

logger = logging.getLogger(__name__)


def get_cooler_info(file_path):
    """Get information of a cooler file.

    Args:
        file_path (str): Path to a cooler file.

    Returns:
        dict: Dictionary containing basic information about the cooler file.
    """
    TILE_SIZE = 256
    CHROM_CUM_LEN = 0
    with h5py.File(file_path, "r") as f:
        max_zoom = f.attrs.get("max-zoom")

        if max_zoom is None:
            logger.info("no zoom found")
            raise ValueError("The `max_zoom` attribute is missing.")

        total_length = CHROM_CUM_LEN
        max_zoom = f.attrs["max-zoom"]
        bin_size = int(f[str(max_zoom)].attrs["bin-size"])

        max_width = bin_size * TILE_SIZE * 2 ** max_zoom

        info = {
            "min_pos": [0.0, 0.0],
            "max_pos": [total_length, total_length],
            "max_zoom": max_zoom,
            "max_width": max_width,
            "bins_per_dimension": TILE_SIZE,
        }

        return info
