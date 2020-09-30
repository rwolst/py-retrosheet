"""Main CLI program to call functions from."""

import click

from .config import config
from .download import download
from .ensure import ensure
from .parse import parse


@click.group(help="CLI for interfacing with Retrosheet.")
def cli():
    pass


cli.add_command(config)
cli.add_command(download)
cli.add_command(ensure)
cli.add_command(parse)
