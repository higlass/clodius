from distutils.core import setup
from distutils.extension import Extension

from Cython.Build import cythonize
from Cython.Distutils import build_ext

import numpy 

setup(name='clodius',
     include_dirs=[numpy.get_include()],
      version='0.2.5',
      description='Tile generation of big data',
      author='Peter Kerpedjiev',
      author_email='pkerpedjiev@gmail.com',
      url='',
      packages=['clodius'],
      setup_requires = [
          'cython>=0.x'
          ],
      extensions = [Extension("*", ["*.pyx"])],
      cmdclass={'build_ext': build_ext},
      scripts=['scripts/make_single_threaded_tiles.py']
     )
