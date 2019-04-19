import numpy as np


def aggregate(x, k):
    x = np.asarray(x)

    if len(x) % k != 0:
        extend = (k - len(x) % k)
        x = np.r_[x, [np.nan] * extend]

    xr = x.reshape((-1, k))
    is_allnan = np.all(np.isnan(xr), axis=1)
    agg = np.nansum(xr, axis=1)

    if np.any(is_allnan):
        if not issubclass(agg.dtype.type, np.floating):
            agg = agg.astype(np.float64)
        agg[is_allnan] = np.nan

    return agg
