import os
import configparser
import sqlalchemy


def load_installed_config():
    """Loads the CONFIG file for the installed package."""
    install_path = os.path.dirname(os.path.abspath(__file__))

    # Load CONFIG.
    config = configparser.ConfigParser()
    config.readfp(open(install_path + '/conf/config.ini'))

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
