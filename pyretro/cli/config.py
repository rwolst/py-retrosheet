"""CLI tools for setting the package configuration."""

import os
import click
import yaml
from pprint import pprint

from ..utils import load_installed_config


@click.group(help="Set configuration values.")
def config():
    pass


@click.command(help="Tool to modify CONFIG file.")
@click.argument('section')
@click.argument('key')
@click.argument('value')
def modify(section, key, value):
    """Modify config file and overwrite the old one."""
    # First get handle to CONFIG file.
    config, paths = load_installed_config()

    # Modify the value.
    config[section][key] = value

    # Overwrite the old CONFIG.
    with open(paths['config'], 'w') as f:
        yaml.dump(config, f)


@click.command(help="Tool to print CONFIG file.")
def show():
    """Modify config file and overwrite the old one."""
    # First get handle to CONFIG file.
    config, paths = load_installed_config()

    pprint(config)


config.add_command(modify)
config.add_command(show)
