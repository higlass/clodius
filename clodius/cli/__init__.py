# -*- coding: utf-8 -*-
from __future__ import division, print_function
import click


# Monkey patch
click.core._verify_python3_env = lambda: None


CONTEXT_SETTINGS = {"help_option_names": ["-h", "--help"]}


@click.group(context_settings=CONTEXT_SETTINGS)
def cli():
    pass


from . import (  # noqa: F401
    # Another approach would be __all__:
    # https://docs.python.org/2/tutorial/modules.html#importing-from-a-package
    aggregate,
    convert,
)
