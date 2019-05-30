from setuptools import setup, find_packages


print("packages:", find_packages())

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
        'cooler>=0.8.0',
        'pybbi==0.2.0',
        'Click']

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
    scripts=[
        'scripts/tsv_to_mrmatrix.py'
    ],
    entry_points={
        'console_scripts': [
                'clodius = clodius.cli.aggregate:cli'
            ]
        }
)
