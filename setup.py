from setuptools import setup, find_packages, Extension

#from distutils.extension import Extension

#from Cython.Distutils import build_ext
#from Cython.Build import cythonize


print("packages:", find_packages())
 
setup_requires = [
        'cython',
        'numpy'
        ]

install_requires = [
        'cython',
        'numpy',
        'negspy',
        'pysam',
        'requests',
        'h5py',
        'pybedtools',
        'pandas',
        'slugid',
        'sortedcontainers',
        'nose',
        'pyBigWig',
        'Click']

def extensions():
    import numpy
    from Cython.Build import cythonize
    clodius_fast = Extension(
        "clodius.fast", ["clodius/fast.pyx"], include_dirs=[
            numpy.get_include()])
    return cythonize([clodius_fast])

def numpy_include():
    import numpy

    return [numpy.get_include()]

class lazy_cythonize(list):
    '''
    Don't try to include these files until absolutely necessary.

    This function is required because at the time of calling the
    scripts in setup_requires may not have been installed.

    This delays their use until after they've been installed.
    '''
    def __init__(self, callback):
        self._list, self.callback = None, callback
    def c_list(self):
        if self._list is None: self._list = self.callback()
        return self._list
    def __iter__(self):
        for e in self.c_list(): yield e
    def __getitem__(self, ii): return self.c_list()[ii]
    def __len__(self): return len(self.c_list())

setup(
    name='clodius',
    include_dirs= lazy_cythonize(numpy_include),
    ext_modules = lazy_cythonize(extensions),
    version='0.4.2',
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
