import math
import numpy as np
cimport numpy as np

from libc.math cimport isnan

def aggregate(np.ndarray[np.float32_t,ndim=1] in_array, int num_to_agg):
    '''
    Calculate a new array which contains the sums of every
    num_to_agg elements of the original array.

    Example:

    aggregate([1,2,3,4], 2) #=> [3,7]

    :param in_array: A numpy array
    :param num_to_agg: The number of elements to aggregate into one
    :return: A numpy array
    '''
    cdef int length = len(in_array)
    cdef np.ndarray[np.float32_t,ndim=1] out_array = np.empty(int(math.ceil(length / float(num_to_agg))), 
                                                              dtype=np.float32)
    out_array.fill(np.nan)
    cdef int i = 0
    cdef int j = 0

    cdef float a = 0
    cdef float b = 0

    #print("num_to_agg:", num_to_agg)
    #print("in_array:", in_array[-10:])
    while i < length:
        out_array[i / num_to_agg] = np.nan;
        j = 0
        while j < num_to_agg and (i+j) < length:
            a = out_array[i / num_to_agg]
            b = in_array[i+j]

            # two nans sum to nan
            if isnan(a) and isnan(b):
                out_array[i / num_to_agg] = np.nan
                j += 1
                continue

            if isnan(a):
                a = 0.
            if isnan(b):
                b = 0.

            #print("a:", a, "b:", b, isnan(a), isnan(b))
            out_array[i / num_to_agg] = a + b
            j += 1
        i += num_to_agg

    return out_array
