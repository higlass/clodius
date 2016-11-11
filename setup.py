from distutils.core import setup

setup(name='clodius',
      version='0.1.2',
      description='Tile generation of big data',
      author='Peter Kerpedjiev',
      author_email='pkerpedjiev@gmail.com',
      url='',
      packages=['clodius'],
      scripts=['scripts/make_single_threaded_tiles.py']
     )
