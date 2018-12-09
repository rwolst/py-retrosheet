"""A CLI program for ensuring that the correct tables exist and creating them
if necessary."""

import os
import configparser as ConfigParser
import sqlalchemy
import click

from ..utils import (connect, load_modified_config)


# Load CONFIG.
CONFIG = load_modified_config()


def execute_sql_file(fname, conn):
    """Executes a full .sql file with SQLAlchemy."""
    # Open the .sql file
    sql_file = open(fname, 'r')

    # Create an empty command string
    sql_command = ''

    # Iterate over all lines in the sql file
    for line in sql_file:
        # Ignore commented lines
        if not line.startswith('--') and line.strip('\n'):
            # Append line to the command string
            sql_command += line.strip('\n')

            # If the command string ends with ';', it is a full statement
            if sql_command.endswith(';'):
                # Try to execute statement and commit it
                conn.execute(sql_command)

                # Finally, clear command string
                sql_command = ''


def ensure(name, table_names, conn, recreate):
    """Check if teams exists and if not create it. If recreate is set
    then we always drop any existing table. The name of the .sql to run
    when creating is in fname and the names of all tables that should exist
    are in table_names."""
    path = CONFIG.get('path', 'sql_path')
    fname = path + '/%s_schema.sql' % name

    if not recreate:
        # Find if table exists.
        sql = """SELECT EXISTS (
                   SELECT 1
                   FROM   information_schema.tables 
                   WHERE  table_name = '%s'
                 )"""

        all_tables_exist = True
        for table_name in table_names:
            res = conn.execute(sql % table_name).fetchone()[0]
            all_tables_exist = all_tables_exist and res

        if all_tables_exist:
            ## Table exists and isn't getting recreated.
            print('%s table exists, not recreating.' % name)
            return
        else:
            ## Table must be created.
            print("%s table doesn't exist, recreating." % name)
            pass

    # If we are here then no matter what we are dropping and creating the table.
    # This is already written in the sql scripts.
    print("%s table recreation." % name)
    execute_sql_file(fname, conn)


@click.group(help="CLI for ensuring tables exist in our database, to allow parser to work.")
def cli():
    pass


@click.command(help="SQL to ensure the Retrosheet tables.")
@click.option('--recreate', is_flag=True, help="Force recreation of tables when ensuring i.e. delete all current data.")
def retro(recreate):
    try:
        conn = connect(CONFIG)
    except Exception as e:
        print('Cannot connect to database: %s' % e)
        raise SystemExit

    verbose = CONFIG.getboolean('debug', 'verbose')
    table_names = ['events', 'games', 'teams', 'rosters']

    ensure('retro', table_names, conn, recreate)

    conn.close()


@click.command(help="SQL to ensure the PeopleIDs table.")
@click.option('--recreate', is_flag=True, help="Force recreation of tables when ensuring i.e. delete all current data.")
def people(recreate):
    try:
        conn = connect(CONFIG)
    except Exception as e:
        print('Cannot connect to database: %s' % e)
        raise SystemExit

    verbose = CONFIG.getboolean('debug', 'verbose')
    table_names = ['peopleids']

    ensure('peopleids', table_names, conn, recreate)

    conn.close()


@click.command(help="SQL to ensure the PlayerIDs table.")
@click.option('--recreate', is_flag=True, help="Force recreation of tables when ensuring i.e. delete all current data.")
def players(recreate):
    try:
        conn = connect(CONFIG)
    except Exception as e:
        print('Cannot connect to database: %s' % e)
        raise SystemExit

    verbose = CONFIG.getboolean('debug', 'verbose')
    table_names = ['playerids']

    ensure('playerids', table_names, conn, recreate)

    conn.close()


@click.command(help="SQL to ensure the Hist_PlayerIDs table.")
@click.option('--recreate', is_flag=True, help="Force recreation of tables when ensuring i.e. delete all current data.")
def hist_players(recreate):
    try:
        conn = connect(CONFIG)
    except Exception as e:
        print('Cannot connect to database: %s' % e)
        raise SystemExit

    verbose = CONFIG.getboolean('debug', 'verbose')
    table_names = ['hist_playerids']

    ensure('hist_playerids', table_names, conn, recreate)

    conn.close()


@click.command(help="SQL to ensure the TeamIDs table.")
@click.option('--recreate', is_flag=True, help="Force recreation of tables when ensuring i.e. delete all current data.")
def teams(recreate):
    try:
        conn = connect(CONFIG)
    except Exception as e:
        print('Cannot connect to database: %s' % e)
        raise SystemExit

    verbose = CONFIG.getboolean('debug', 'verbose')
    table_names = ['teamids']

    ensure('teamids', table_names, conn, recreate)

    conn.close()


@click.command(help="SQL to ensure the ParksIDs table.")
@click.option('--recreate', is_flag=True, help="Force recreation of tables when ensuring i.e. delete all current data.")
def parks(recreate):
    try:
        conn = connect(CONFIG)
    except Exception as e:
        print('Cannot connect to database: %s' % e)
        raise SystemExit

    verbose = CONFIG.getboolean('debug', 'verbose')
    table_names = ['parkids']

    ensure('parkids', table_names, conn, recreate)

    conn.close()


cli.add_command(retro)
cli.add_command(people)
cli.add_command(players)
cli.add_command(hist_players)
cli.add_command(teams)
cli.add_command(parks)
