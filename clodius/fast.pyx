import math
import numpy as np
cimport numpy as np

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
    cdef np.ndarray[np.float32_t,ndim=1] out_array = np.zeros(int(math.ceil(length / float(num_to_agg))), 
                                                              dtype=np.float32)
    cdef int i = 0
    cdef int j = 0

    #print "length:", length, "num_to_agg:", num_to_agg, "new length:", len(out_array), math.ceil(length / num_to_agg) , 

    while i < length:
        #print "i/num_to_agg:", length, i, i / num_to_agg
        out_array[i / num_to_agg] = 0;
        j = 0
        while j < num_to_agg and (i+j) < length:
            out_array[i / num_to_agg] += in_array[i+j]
            j += 1
        i += num_to_agg

    return out_array
