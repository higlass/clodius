from distutils.core import setup
from Cython.Build import cythonize

import numpy 

setup(name='clodius',
    ext_modules=cythonize("clodius/fast.pyx"),
     include_dirs=[numpy.get_include()],
      version='0.2.0',
      description='Tile generation of big data',
      author='Peter Kerpedjiev',
      author_email='pkerpedjiev@gmail.com',
      url='',
      packages=['clodius'],
      scripts=['scripts/make_single_threaded_tiles.py']
     )
