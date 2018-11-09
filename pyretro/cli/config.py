"""CLI tools for setting the package configuration."""

import os
import configparser as ConfigParser
import click

from ..utils import load_raw_config

@click.group(help="CLI for setting configuration values.")
def cli():
    pass


@click.command(help="Tool to modify CONFIG file.")
@click.argument('section')
@click.argument('key')
@click.argument('value')
def modify(section, key, value):
    """Modify config file and overwrite the old one."""
    # First get handle to CONFIG file.
    config, _, config_path = load_raw_config()

    # Modify the value.
    config.set(section, key, value)

    # Overwrite the old CONFIG.
    with open(config_path, 'w') as f:
        config.write(f)


cli.add_command(modify)
