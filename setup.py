from setuptools import setup, find_packages
from distutils import sysconfig

#from distutils.extension import Extension

#from Cython.Distutils import build_ext
#from Cython.Build import cythonize

setup_requires = [
    'numpy',
]

install_requires = [
    'numpy',
    'negspy',
    'pysam',
    'dask',
    'requests',
    'h5py',
    'pandas',
    'slugid',
    'sortedcontainers',
    'nose',
    'cooler>=0.8.5',
    'pybbi>=0.2.0',
    'Click>=7'
]

setup(
    name='clodius',
    version='0.10.7',
    description='Tile generation for big data',
    author='Peter Kerpedjiev',
    author_email='pkerpedjiev@gmail.com',
    url='',
    packages=['clodius', 'clodius.cli', 'clodius.tiles'],
    setup_requires=setup_requires,
    install_requires=install_requires,
    entry_points={
        'console_scripts': [
            'clodius = clodius.cli.aggregate:cli',
        ]
    }
)
