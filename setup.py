from distutils.core import setup
from distutils.extension import Extension

from Cython.Build import cythonize
from Cython.Distutils import build_ext

import numpy 

setup(name='clodius',
     include_dirs=[numpy.get_include()],
      ext_modules = cythonize([Extension("clodius.fast", ["clodius/fast.pyx"])]),
      version='0.3.1',
      description='Tile generation of big data',
      author='Peter Kerpedjiev',
      author_email='pkerpedjiev@gmail.com',
      url='',
      packages=['clodius'],
      setup_requires = [
          'cython>=0.x'
          ],
      scripts=['scripts/make_single_threaded_tiles.py', 'scripts/tile_bigWig.py']
     )
