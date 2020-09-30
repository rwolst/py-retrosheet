import os
import sqlalchemy
import sys
import yaml


class ConfigException(Exception):
    """Exception for errors in config file."""
    pass


def get_paths():
    """Gets the following paths:
        install_path: Where the package is installed.
        virtual_path: The path for the current virtual environment we are in.
        config_path: The path to the CONFIG file.
        sql_path: The path to the SQL scripts directory.
    """
    # Install and virtual are easy.
    install_path = os.path.dirname(os.path.abspath(__file__))
    virtual_path = os.environ['VIRTUAL_ENV']
    system_path = sys.prefix

    # The CONFIG is either installed in the installation path (if editable
    # install) or in env/conf. We check them both.
    install_config_path = install_path + '/conf/config.yml'
    virtual_config_path = virtual_path + '/pyretro/conf/config.yml.dist'
    system_config_path = system_path + '/pyretro/conf/config.yml.dist'
    if os.path.exists(install_config_path):
        config_path = install_config_path
    elif os.path.exists(virtual_config_path):
        config_path = virtual_config_path
    elif os.path.exists(system_config_path):
        config_path = system_config_path
    else:
        raise Exception("No CONFIG file found.")

    # Similarly for the SQL scripts.
    install_sql_path = install_path + '/sql/postgres'
    virtual_sql_path = virtual_path + '/pyretro/sql'
    system_sql_path = system_path + '/pyretro/sql'
    if os.path.exists(install_sql_path):
        sql_path = install_sql_path
    elif os.path.exists(virtual_sql_path):
        sql_path = virtual_sql_path
    elif os.path.exists(system_sql_path):
        sql_path = system_sql_path
    else:
        raise Exception("No SQL path found.")

    return {
        'install': install_path,
        'virtual': virtual_path,
        'system': system_path,
        'config': config_path,
        'sql': sql_path
    }


def load_installed_config():
    """Loads the CONFIG file for the installed package."""
    paths = get_paths()

    # Load CONFIG.
    config = yaml.safe_load(
        open(
            paths['config'],
            'r'
        )
    )

    return config, paths


def load_modified_config():
    """Loads the CONFIG file for the installed package and adds extra
    sections."""
    config, paths = load_installed_config()

    # Add install path to the config.
    config['path'] = {}
    config['path']['install_path'] = paths['install']

    # Add config path to the config.
    config['path']['config_path'] = paths['config']

    # Add sql path to the config.
    config['path']['sql_path'] = paths['sql']

    # Make and add download path to the config.
    download_path = config['download']['directory']

    if config['download']['use_tmp']:
        download_path = '/tmp/' + download_path
    else:
        download_path = os.path.abspath(download_path)

    # Test for existence of download directory
    # Create if does not exist
    if not os.path.exists(download_path):
        print(f"Directory {download_path} does not exist, creating...")
        os.makedirs(download_path)

    config['path']['download_path'] = download_path

    return config


def connect(config):
    """Return a connection to the database."""
    try:
        ENGINE = config['database']['engine']
        DATABASE = config['database']['database']

        HOST = config['database']['host']
        USER = config['database']['user']
        PASSWORD = config['database']['password']

    except KeyError:
        raise ConfigException(
            "Need to define engine, user, password, host, and database"
            " parameters"
        )

    if ENGINE == 'sqlite':
        dbString = f"{ENGINE}:///{DATABASE}"
    else:
        if USER and PASSWORD:
            dbString = f"{ENGINE}://{USER}:{PASSWORD}@{HOST}/{DATABASE}"
        elif USER:
            dbString = f"{ENGINE}://{USER}@{HOST}/{DATABASE}"
        else:
            dbString = f"{ENGINE}://{HOST}/{DATABASE}"

    db = sqlalchemy.create_engine(dbString)
    conn = db.connect()

    return conn
