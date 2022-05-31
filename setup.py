import io
import os

from setuptools import setup
from pathlib import Path

HERE = os.path.dirname(os.path.abspath(__file__))
long_description = (Path(HERE) / "README.md").read_text()

def read(*parts, **kwargs):
    filepath = os.path.join(HERE, *parts)
    encoding = kwargs.pop("encoding", "utf-8")
    with io.open(filepath, encoding=encoding) as fh:
        text = fh.read()
    return text


def get_requirements(path):
    content = read(path)
    return [req for req in content.split("\n") if req != "" and not req.startswith("#")]


setup_requires = ["numpy"]

install_requires = get_requirements("requirements.txt")

setup(
    name="clodius",
    version="0.19.0",
    description="Tile generation for big data",
    author="Peter Kerpedjiev",
    author_email="pkerpedjiev@gmail.com",
    url="",
    packages=["clodius", "clodius.cli", "clodius.tiles"],
    setup_requires=setup_requires,
    install_requires=install_requires,
    scripts=["scripts/tsv_to_mrmatrix.py"],
    entry_points={"console_scripts": ["clodius = clodius.cli.aggregate:cli"]},
    long_description=long_description,
    long_description_content_type='text/markdown'
)
