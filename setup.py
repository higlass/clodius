from setuptools import setup, find_packages
from distutils import sysconfig

#from distutils.extension import Extension

#from Cython.Distutils import build_ext
#from Cython.Build import cythonize


print("packages:", find_packages())
 
setup_requires = [
        'cython',
        'numpy',
        ]

install_requires = [
        'numpy',
        'negspy',
        'pysam',
        'requests',
        'h5py',
        'pandas',
        'slugid',
        'sortedcontainers',
        'nose',
        'Click']

setup(
    name='clodius',
    version='0.9.4',
    description='Tile generation for big data',
    author='Peter Kerpedjiev',
    author_email='pkerpedjiev@gmail.com',
    url='',
    packages=['clodius', 'clodius.cli'],
    setup_requires=setup_requires,
    install_requires=install_requires,
    entry_points={
        'console_scripts': [
            'clodius = clodius.cli.aggregate:cli',
            ]
        }
)
