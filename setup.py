from setuptools import setup, find_packages, Extension
import os.path as op
import sys
import io

#from distutils.extension import Extension
#from Cython.Distutils import build_ext
#from Cython.Build import cythonize
#print("packages:", find_packages())

classifiers = """
Development Status :: 4 - Beta
License :: OSI Approved
Programming Language :: Python
Programming Language :: Python :: 2
Programming Language :: Python :: 2.7
Programming Language :: Python :: 3
Programming Language :: Python :: 3.4
Programming Language :: Python :: 3.5
Programming Language :: Python :: 3.6
Programming Language :: Python :: Implementation :: CPython
"""

setup_requires = [
    'cython',
    'numpy'
]

install_requires = [
    'cython',
    'numpy',
    'h5py',
    'pandas',
    'requests',
    'nose',
    'Click'
    'sortedcontainers',
    'slugid',
    'pysam',
    'pyBigWig',
    'negspy',

]


def get_long_description():
    filepath = op.join(op.dirname(__file__), 'README.md')
    with io.open(filepath, encoding='utf-8') as f:
        descr = f.read()
    try:
        import pypandoc
        descr = pypandoc.convert_text(descr, to='rst', format='md')
    except (IOError, ImportError):
        pass
    return descr


def get_ext_modules():
    from Cython.Build import cythonize
    import numpy

    extensions = [
        Extension(
            "clodius.fast", 
            ["clodius/fast.pyx"], 
            include_dirs=[
                op.join(sys.prefix, 'include'),
                numpy.get_include()
            ]
        )
    ]
    return cythonize(extensions)


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
    version='0.4.5',
    packages=['clodius', 'clodius.cli'],
    ext_modules=lazy_cythonize(get_ext_modules),
    description='Tile generation for big data',
    long_description=get_long_description(),
    url='https://github.com/hms-dbmi/clodius',
    author='Peter Kerpedjiev',
    author_email='pkerpedjiev@gmail.com',
    classifiers=[s.strip() for s in classifiers.split('\n') if s],
    setup_requires=setup_requires,
    install_requires=install_requires,
    entry_points={
        'console_scripts': [
            'clodius = clodius.cli.aggregate:cli',
            ]
        }
)
