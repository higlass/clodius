from setuptools import setup
import os
import io


HERE = os.path.dirname(os.path.abspath(__file__))


def read(*parts, **kwargs):
    filepath = os.path.join(HERE, *parts)
    encoding = kwargs.pop('encoding', 'utf-8')
    with io.open(filepath, encoding=encoding) as fh:
        text = fh.read()
    return text


def get_requirements(path):
    content = read(path)
    return [
        req
        for req in content.split("\n")
        if req != '' and not req.startswith('#')
    ]


setup_requires = [
    'numpy',
]

install_requires = get_requirements('requirements.txt')

setup(
    name='clodius',
    version='0.10.12',
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
