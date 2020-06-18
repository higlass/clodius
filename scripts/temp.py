import time
import clodius.fast
import numpy as np
import pyximport

pyximport.install(setup_args={"include_dirs": np.get_include()})


x = np.array(range(2 ** 16))
t1 = time.time()
t2 = time.time()
print(clodius.fast.aggregate(x, 8))
print("t2:", t2 - t1)
