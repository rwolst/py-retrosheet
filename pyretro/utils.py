import os
import configparser
import sqlalchemy


def load_raw_config():
    """Loads the raw config file without any modifications."""
    # The CONFIG is either installed in the installation path (if editable
    # install) or in env/config. We check them both.
    install_path = os.path.dirname(os.path.abspath(__file__))
    virtual_path = os.environ['VIRTUAL_ENV']

    # Load CONFIG.
    install_config_path = install_path + '/conf/pyretro_config.ini'
    virtual_config_path = virtual_path + '/conf/pyretro_config.ini.dist'
    config = configparser.ConfigParser()
    if os.path.exists(install_config_path):
        config.readfp(open(install_config_path))
        config_path = install_config_path
    elif os.path.exists(virtual_config_path):
        config.readfp(open(virtual_config_path))
        config_path = virtual_config_path
    else:
        raise Exception("No CONFIG file found.")

    return config, install_path, config_path


def load_installed_config():
    """Loads the CONFIG file for the installed package and adds extra
    sections."""
    config, install_path, config_path = load_raw_config()

    # Add install path to the config.
    config.add_section('path')
    config.set('path', 'install_path', install_path)

    # Make and add download path to the config.
    download_path = config.get('download', 'directory')
    if config.get('download', 'use_tmp') == 'True':
        download_path = '/tmp/' + download_path
    else:
        download_path = os.path.abspath(download_path)

    # Test for existence of download directory
    # Create if does not exist
    if not os.path.exists(download_path):
        print("Directory %s does not exist, creating..." % download_path)
        os.makedirs(download_path)

    config.set('path', 'download_path', download_path)

    return config


def connect(config):
    """Return a connection to the database."""
    try:
        ENGINE = config.get('database', 'engine')
        DATABASE = config.get('database', 'database')

        HOST = None if not config.has_option('database', 'host') else config.get('database', 'host')
        USER = None if not config.has_option('database', 'user') else config.get('database', 'user')
        SCHEMA = None if not config.has_option('database', 'schema') else config.get('database', 'schema')
        PASSWORD = None if not config.has_option('database', 'password') else config.get('database', 'password')
    except configparser.NoOptionError:
        print('Need to define engine, user, password, host, and database parameters')
        raise SystemExit

    if ENGINE == 'sqlite':
        dbString = ENGINE + ':///%s' % (DATABASE)
    else:
        if USER and PASSWORD:
            dbString = ENGINE + '://%s:%s@%s/%s' % (USER, PASSWORD, HOST, DATABASE)
        elif USER:
            dbString = ENGINE + '://%s@%s/%s' % (USER, HOST, DATABASE)
        else:
            dbString = ENGINE + '://%s/%s' % (HOST, DATABASE)

    db = sqlalchemy.create_engine(dbString)
    conn = db.connect()

    return conn
