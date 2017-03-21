from setuptools import setup, find_packages

from distutils.extension import Extension

from Cython.Build import cythonize


import numpy
print("packages:", find_packages())

setup(
    name='clodius',
    include_dirs=[numpy.get_include()],
    ext_modules = cythonize([Extension(
        "clodius.fast", ["clodius/fast.pyx"], include_dirs=[
            numpy.get_include()])]),
    version='0.4.0',
    description='Tile generation for big data',
    author='Peter Kerpedjiev',
    author_email='pkerpedjiev@gmail.com',
    url='',
    packages=['clodius', 'clodius.cli'],
    install_requires=['Click'],
    entry_points={
        'console_scripts': [
            'clodius = clodius.cli.aggregate:cli',
            ]
        }
)
