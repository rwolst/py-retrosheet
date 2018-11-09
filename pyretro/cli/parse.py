"""CLI tools for parsing downloaded data into the Retrosheet database. As we
will be calling this may times, potentially when the database already exists,
we need to make sure it can handle updating correctly."""

import os
import subprocess
import time
import glob
import re
import csv
import click
import pandas as pd

from ..utils import (connect, load_modified_config)

# Load CONFIG.
CONFIG = load_modified_config()


def parse_rosters(fname, conn, bound_param):
    """Insert/Update rosters (in fname) into the database. The bound_param
    argument represents the default variable argument in SQL that the engine
    understands e.g. '?' for SQLite or '%s' for PostgreSQL."""
    print("Processing %s" % fname)

    # Extract year from the file name.
    try:
        year = re.search(r"\d{4}", os.path.basename(fname)).group(0)
    except:
        print('cannot get year from roster file %s' % fname)
        return None

    reader = csv.reader(open(fname))

    for row in reader:
        row.insert(0, year) # Insert year

        sql = 'SELECT * FROM rosters WHERE year = %s AND player_id = %s AND team_tx = %s'
        res = conn.execute(sql, [row[0], row[1], row[6]])

        ## Avoid adding existing data.
        if res.rowcount == 1:
            continue

        sql = "INSERT INTO rosters VALUES (%s)" % ", ".join([bound_param] * len(row))
        conn.execute(sql, row)

    return True


def parse_teams(fname, conn, bound_param):
    """Insert/Update teams (in fname) into the database. The bound_param
    argument represents the default variable argument in SQL that the engine
    understands e.g. '?' for SQLite or '%s' for PostgreSQL."""
    print("Processing %s" % fname)

    reader = csv.reader(open(fname))
    for row in reader:
        if len(row) != 4:
            continue

        sql = 'SELECT * FROM teams WHERE team_id = %s'
        res = conn.execute(sql, [row[0]])

        ## Avoid adding existing data.
        if res.rowcount == 1:
            continue

        sql = "INSERT INTO teams VALUES (%s)" % ", ".join([bound_param] * len(row))
        conn.execute(sql, row)


def parse_games(fname, conn, bound_param):
    """Insert/Update games (in fname) into the database. The bound_param
    argument represents the default variable argument in SQL that the engine
    understands e.g. '?' for SQLite or '%s' for PostgreSQL."""
    print("Processing %s" % fname)

    # Extract year from the fname name.
    try:
        year = re.search(r"\d{4}", os.path.basename(fname)).group(0)
    except:
        print('Cannot get year from game file %s' % fname)
        return None
 
    if conn.engine.driver == 'psycopg2':
        ## Avoid adding existing data.
        conn.execute('DELETE FROM games WHERE game_id LIKE \'%%' + year + '%%\'')

        ## In order to get access to the cpy_expert methods, we have to create
        ## a raw database connection.
        fake_conn = conn.engine.raw_connection()
        fake_cur = fake_conn.cursor()
        f = open(fname, 'rb')
        fake_cur.copy_expert('COPY games FROM STDOUT WITH CSV HEADER', f)
        fake_conn.commit()
        #fake_conn.close()
        conn.execute('COMMIT')
    else:
        reader = csv.reader(open(fname))
        headers = reader.next()
        for row in reader:
            sql = 'SELECT * FROM games WHERE game_id = %s'
            res = conn.execute(sql, [row[0]])

            ## Avoid adding existing data.
            if res.rowcount == 1:
                continue

            sql = 'INSERT INTO games(%s) VALUES(%s)' % (','.join(headers), ','.join([bound_param] * len(headers)))
            conn.execute(sql, row)


def parse_events(fname, conn, bound_param):
    """Insert/Update events (in fname) into the database. The bound_param
    argument represents the default variable argument in SQL that the engine
    understands e.g. '?' for SQLite or '%s' for PostgreSQL."""
    print("Processing %s" % fname)

    # Extract year from the file name.
    try:
        year = re.search(r"\d{4}", os.path.basename(fname)).group(0)
    except:
        print('cannot get year from event file %s' % fname)
        return None

    if conn.engine.driver == 'psycopg2':
        ## Avoid adding existing data.
        conn.execute('DELETE FROM events WHERE game_id LIKE \'%%' + year + '%%\'')

        ## In order to get access to the cpy_expert methods, we have to create
        ## a raw database connection.
        fake_conn = conn.engine.raw_connection()
        fake_cur = fake_conn.cursor()
        f = open(fname, 'rb')
        fake_cur.copy_expert('COPY events FROM STDOUT WITH CSV HEADER', f)
        fake_conn.commit()
        #fake_conn.close()
        conn.execute('COMMIT')
    else:
        reader = csv.reader(open(fname))
        headers = reader.next()
        for row in reader:
            sql = 'SELECT * FROM events WHERE game_id = %s AND event_id = %s'
            res = conn.execute(sql, [row[0], row[96]])

            ## Avoid adding existing data.
            if res.rowcount == 1:
                return True

            sql = 'INSERT INTO events(%s) VALUES(%s)' % (','.join(headers), ','.join([bound_param] * len(headers)))
            conn.execute(sql, row)


def parse_people(fname, conn, bound_param):
    """Insert/Update peopleIDs (in fname) into the database. The bound_param
    argument represents the default variable argument in SQL that the engine
    understands e.g. '?' for SQLite or '%s' for PostgreSQL."""
    print("Processing %s" % fname)

    if conn.engine.driver == 'psycopg2':
        ## We have an issue that we cannot simply delete all data and re-copy
        ## the new data, as we will have hanging ravenholm-... references in
        ## events whose player they refer to has been deleted.
        conn.execute('CREATE TABLE peopleids_temp as SELECT * FROM peopleids LIMIT 0')

        ## In order to get access to the copy_expert methods, we have to create
        ## a raw database connection.
        fake_conn = conn.engine.raw_connection()
        fake_cur = fake_conn.cursor()
        f = open(fname, 'rb')
        fake_cur.copy_expert('COPY peopleids_temp FROM STDOUT WITH CSV HEADER', f)
        fake_conn.commit()
        #fake_conn.close()
        conn.execute('COMMIT')

        # Find the new players to insert by their uuid.
        # For all new players to insert, we check if their MLBAM already
        # exists.
        # If it does, we replace the old entry as well as updating all other
        # references to the retro id in our database.

        # Now update peopleids and on conflict (i.e. uuid already exists) ignore.
        conn.execute('INSERT INTO peopleids SELECT * FROM peopleids_temp ON CONFLICT DO NOTHING')

        conn.execute('DROP TABLE peopleids_temp')

        # Find any PlayerIDs with ravenholm-... retro_ID that we may now be
        # able to update.
        sql = """SELECT derived.mlb_id, derived.retro_id, peopleids.key_mlbam, peopleids.key_retro FROM (SELECT mlb_id, retro_id FROM playerids WHERE retro_id LIKE 'ravenholm%%') as derived LEFT JOIN peopleids ON (derived.mlb_id = peopleids.key_mlbam) WHERE peopleids.key_retro is not null"""
        res = conn.execute(sql)
        updates = pd.DataFrame(res.fetchall(), columns=res.keys())

        ### Get the column names to update.
        ### Note this also gets team IDs, game IDs etc. but they should never be
        ### called ravenholm-... as it is reserved for people.
        sql = """SELECT column_name FROM information_schema.columns WHERE table_name='events' AND column_name LIKE '%%id%%'"""
        res = conn.execute(sql)
        events_id_columns = [i[0] for i in res.fetchall()]

        sql = """SELECT column_name FROM information_schema.columns WHERE table_name='games' AND column_name LIKE '%%id%%'"""
        res = conn.execute(sql)
        games_id_columns = [i[0] for i in res.fetchall()]

        ### Now loop.
        for idx, row in updates.iterrows():
            ### Update all instances of retro_id to retro_id_temp
            for col in events_id_columns:
                sql = """UPDATE events SET %s = '%s' WHERE %s = '%s'"""
                conn.execute(sql % (col, row['key_retro'], col, row['retro_id']))

            for col in games_id_columns:
                sql = """UPDATE games SET %s = '%s' WHERE %s = '%s'"""
                conn.execute(sql % (col, row['key_retro'], col, row['retro_id']))

            sql = """UPDATE playerIDs SET retro_id = '%s' WHERE mlb_id = '%s'"""
            conn.execute(sql % (row['key_retro'], row['mlb_id']))

    else:
        raise Exception("Only implemented for Postgres.")


def parse_players(fname, conn, bound_param):
    """Insert/Update playerIDs (in fname) into the database. The bound_param
    argument represents the default variable argument in SQL that the engine
    understands e.g. '?' for SQLite or '%s' for PostgreSQL."""
    print("Processing %s" % fname)

    if conn.engine.driver == 'psycopg2':
        # The mlb_id in playerIDs matches with key_mlbam in peopleIDs. Hence,
        # we can find duplicates by looking for duplicated mlb_id.

        ## We have an issue that we cannot simply delete all data and re-copy
        ## the new data, as we will have hanging ravenholm-... references in
        ## events whose player they refer to has been deleted.
        conn.execute('CREATE TABLE playerids_temp as SELECT * FROM playerids LIMIT 0')

        ## In order to get access to the copy_expert methods, we have to create
        ## a raw database connection.
        fake_conn = conn.engine.raw_connection()
        fake_cur = fake_conn.cursor()
        f = open(fname, 'rb')
        fake_cur.execute("SET CLIENT_ENCODING TO 'LATIN1';")
        fake_cur.copy_expert('COPY playerids_temp FROM STDOUT WITH CSV HEADER', f)
        fake_conn.commit()
        #fake_conn.close()
        conn.execute('COMMIT')

        # Find the new players to insert by their mlb_id.
        # Find any conflicting players with the same mlb_id but different
        # retro_id.
        # If it does, we replace the old entry as well as updating all other
        # references to the retro id in our database.

        # Now update playerids and on conflict ignore.
        conn.execute('INSERT INTO playerids SELECT * FROM playerids_temp ON CONFLICT DO NOTHING')

        # Find conflicting players.
        sql = """SELECT * FROM (SELECT playerids.mlb_id as mlb_id, playerids.retro_id as retro_id, playerids_temp.retro_id as retro_id_temp FROM playerids INNER JOIN playerids_temp ON (playerids.mlb_id = playerids_temp.mlb_id)) as derived WHERE retro_id != retro_id_temp"""
        res = conn.execute(sql)
        duplicates = pd.DataFrame(res.fetchall(), columns=res.keys())

        conn.execute('DROP TABLE playerids_temp')

        ### Get the column names to update.
        ### Note this also gets team IDs, game IDs etc. but they should never be
        ### called ravenholm-... as it is reserved for people.
        sql = """SELECT column_name FROM information_schema.columns WHERE table_name='events' AND column_name LIKE '%%id%%'"""
        res = conn.execute(sql)
        events_id_columns = [i[0] for i in res.fetchall()]

        sql = """SELECT column_name FROM information_schema.columns WHERE table_name='games' AND column_name LIKE '%%id%%'"""
        res = conn.execute(sql)
        games_id_columns = [i[0] for i in res.fetchall()]

        ### Now loop.
        for idx, row in duplicates.iterrows():
            ### Update all instances of retro_id to retro_id_temp
            for col in events_id_columns:
                sql = """UPDATE events SET %s = '%s' WHERE %s = '%s'"""
                conn.execute(sql % (col, row['retro_id_temp'], col, row['retro_id']))

            for col in games_id_columns:
                sql = """UPDATE games SET %s = '%s' WHERE %s = '%s'"""
                conn.execute(sql % (col, row['retro_id_temp'], col, row['retro_id']))

            sql = """UPDATE playerIDs SET retro_id = '%s' WHERE mlb_id = '%s'"""
            conn.execute(sql % (row['retro_id_temp'], row['mlb_id']))

    else:
        raise Exception("Only implemented for Postgres.")


def parse_hist_players(fname, conn, bound_param):
    """Insert/Update historical playerIDs (in fname) into the database. It
    requires the playerIDs table to already exist i.e. by calling
    parse_players. The bound_param argument represents the default variable
    argument in SQL that the engine understands e.g. '?' for SQLite or '%s' for
    PostgreSQL."""
    print("Processing %s" % fname)

    if conn.engine.driver == 'psycopg2':
        conn.execute('CREATE TABLE hist_playerids_temp as SELECT * FROM hist_playerids LIMIT 0')

        ## In order to get access to the copy_expert methods, we have to create
        ## a raw database connection.
        fake_conn = conn.engine.raw_connection()
        fake_cur = fake_conn.cursor()
        f = open(fname, 'rb')
        fake_cur.copy_expert('COPY hist_playerids_temp FROM STDOUT WITH CSV HEADER', f)
        fake_conn.commit()
        #fake_conn.close()
        conn.execute('COMMIT')

        # Now update playerids and on conflict ignore.
        conn.execute('INSERT INTO hist_playerids SELECT * FROM hist_playerids_temp ON CONFLICT DO NOTHING')

        conn.execute('DROP TABLE hist_playerids_temp')
    else:
        raise Exception("Only implemented for Postgres.")

    # It appears that the playerid of hist_playerids is simply the lahman_id of
    # playerids and that the hist_playerids is a superset of playerids.
    #sql = "INSERT INTO PlayerIDs (lahman_id, retro_id, throws, bats) SELECT playerID, retroID, throws, bats FROM hist_playerids ON CONFLICT DO NOTHING;"
    #conn.execute(sql)

    ## Finally create ravenholm-... IDs for MLBAM players with no retro ID.
    #sql = "UPDATE playerids SET retro_id = 'ravenholm-' || mlb_id WHERE retro_id IS NULL AND mlb_id is NOT NULL"
    #conn.execute(sql)


def parse_teamids(fname, conn, bound_param):
    """Insert/Update teamsIDs (in fname) into the database. The bound_param
    argument represents the default variable argument in SQL that the engine
    understands e.g. '?' for SQLite or '%s' for PostgreSQL."""
    print("Processing %s" % fname)

    if conn.engine.driver == 'psycopg2':
        ## Always delete all data and re-add.
        #print("""Warning: deleting all previous peopleids in update. This may
        #      not be want we want especially if we have created ravenholm-...
        #      players.""")

        conn.execute('TRUNCATE TABLE teamids')

        ## In order to get access to the copy_expert methods, we have to create
        ## a raw database connection.
        fake_conn = conn.engine.raw_connection()
        fake_cur = fake_conn.cursor()
        f = open(fname, 'rb')
        fake_cur.execute("SET CLIENT_ENCODING TO 'LATIN1';")
        fake_cur.copy_expert('COPY teamids FROM STDOUT WITH CSV HEADER', f)
        fake_conn.commit()
        #fake_conn.close()
        conn.execute('COMMIT')
    else:
        reader = csv.reader(open(fname))
        headers = reader.next()
        for row in reader:
            sql = 'SELECT * FROM teamids WHERE key_uuid = %s'
            res = conn.execute(sql, [row[1]])

            ## Avoid adding existing data.
            if res.rowcount == 1:
                return True

            sql = 'INSERT INTO teamids(%s) VALUES(%s)' % (','.join(headers), ','.join([bound_param] * len(headers)))
            conn.execute(sql, row)


@click.group(help="CLI for parsing downloaded files into the database.")
def cli():
    pass


@click.command(help="Parse the Retrosheet downloads.")
def retro():
    try:
        conn = connect(CONFIG)
    except Exception as e:
        print('Cannot connect to database: %s' % e)
        raise SystemExit

    useyear     = False # Use a single year or all years
    verbose     = CONFIG.getboolean('debug', 'verbose')
    chadwick    = os.path.abspath(CONFIG.get('chadwick', 'directory'))
    path        = CONFIG.get('path', 'download_path')
    csvpath     = '%s/csv' % path
    fnames      = []
    years       = []
    bound_param = '?' if CONFIG.get('database', 'engine') == 'sqlite' else '%s'
    modules     = ['teams', 'rosters', 'events', 'games'] # items to process

    if not os.path.exists(chadwick) \
        or not os.path.exists('%s/cwevent' % chadwick) \
        or not os.path.exists('%s/cwgame' % chadwick):
        print('chadwick does not exist in %s - exiting' % chadwick)
        raise SystemExit

    if not os.path.exists(path + '/csv'):
        os.makedirs(path + '/csv')

    for fname in glob.glob("%s/*.EV*" % path):
        fnames.append(fname)

    for fname in fnames:
        year = re.search(r"^\d{4}", os.path.basename(fname)).group(0)
        if year not in years:
            years.append(int(year))

    os.chdir(path)
    for year in years:
        if not os.path.isfile('%s/events-%d.csv' % (csvpath, year)):
            cmd = "%s/cwevent -q -n -f 0-96 -x 0-62 -y %d %d*.EV* > %s/events-%d.csv" % (chadwick, year, year, csvpath, year)
            if(verbose):
                print("calling '" + cmd + "'")
            subprocess.call(cmd, shell=True)

        if not os.path.isfile('%s/games-%d.csv' % (csvpath, year)):
            cmd = "%s/cwgame -q -n -f 0-83 -y %d %d*.EV* > %s/games-%d.csv" % (chadwick, year, year, csvpath, year)
            if(verbose):
                print("calling '" + cmd + "'")
            subprocess.call(cmd, shell=True)

    if 'teams' in modules:
        mask = "TEAM*" if not useyear else "TEAM%s*" % years[0]
        for fname in glob.glob(mask):
            parse_teams(fname, conn, bound_param)

    if 'rosters' in modules:
        mask = "*.ROS" if not useyear else "*%s*.ROS" % years[0]
        for fname in glob.glob(mask):
            parse_rosters(fname, conn, bound_param)

    if 'games' in modules:
        mask = '%s/games-*.csv' % csvpath if not useyear else '%s/games-%s*.csv' % (csvpath, years[0])
        for fname in glob.glob(mask):
            parse_games(fname, conn, bound_param)

    if 'events' in modules:
        mask = '%s/events-*.csv' % csvpath if not useyear else '%s/events-%s*.csv' % (csvpath, years[0])
        for fname in glob.glob(mask):
            parse_events(fname, conn, bound_param)

    conn.close()


@click.command(help="Parse the PeopleIDs download.")
#@click.option('--recreate', is_flag=True, help="Recreates the tables from fresh, should only be used if also recreating retro tables to avoid hanging references to ravenholm-... players.")
def people():
    try:
        conn = connect(CONFIG)
    except Exception as e:
        print('Cannot connect to database: %s' % e)
        raise SystemExit

    useyear     = False # Use a single year or all years
    verbose     = CONFIG.getboolean('debug', 'verbose')
    chadwick    = os.path.abspath(CONFIG.get('chadwick', 'directory'))
    path        = CONFIG.get('path', 'download_path')
    csvpath     = '%s/csv' % path
    fnames      = []
    years       = []
    bound_param = '?' if CONFIG.get('database', 'engine') == 'sqlite' else '%s'

    os.chdir(path) # Chadwick seems to need to be in the directory

    parse_people('people.csv', conn, bound_param)

    conn.close()


@click.command(help="Parse the PlayerIDs download.")
#@click.option('--recreate', is_flag=True, help="Recreates the tables from fresh, should only be used if also recreating retro tables to avoid hanging references to ravenholm-... players.")
def players():
    try:
        conn = connect(CONFIG)
    except Exception as e:
        print('Cannot connect to database: %s' % e)
        raise SystemExit

    useyear     = False # Use a single year or all years
    verbose     = CONFIG.getboolean('debug', 'verbose')
    chadwick    = os.path.abspath(CONFIG.get('chadwick', 'directory'))
    path        = CONFIG.get('path', 'download_path')
    csvpath     = '%s/csv' % path
    fnames      = []
    years       = []
    bound_param = '?' if CONFIG.get('database', 'engine') == 'sqlite' else '%s'

    os.chdir(path) # Chadwick seems to need to be in the directory

    parse_players('players.csv', conn, bound_param)

    conn.close()


@click.command(help="Parse the Hist_PlayerIDs download.")
#@click.option('--recreate', is_flag=True, help="Recreates the tables from fresh, should only be used if also recreating retro tables to avoid hanging references to ravenholm-... players.")
def hist_players():
    try:
        conn = connect(CONFIG)
    except Exception as e:
        print('Cannot connect to database: %s' % e)
        raise SystemExit

    useyear     = False # Use a single year or all years
    verbose     = CONFIG.getboolean('debug', 'verbose')
    chadwick    = os.path.abspath(CONFIG.get('chadwick', 'directory'))
    path        = CONFIG.get('path', 'download_path')
    csvpath     = '%s/csv' % path
    fnames      = []
    years       = []
    bound_param = '?' if CONFIG.get('database', 'engine') == 'sqlite' else '%s'

    os.chdir(path) # Chadwick seems to need to be in the directory

    parse_hist_players('hist_players.csv', conn, bound_param)

    conn.close()


@click.command(help="Parse the TeamIDs download.")
#@click.option('--recreate', is_flag=True, help="Recreates the tables from fresh, should only be used if also recreating retro tables to avoid hanging references to ravenholm-... players.")
def teams():
    try:
        conn = connect(CONFIG)
    except Exception as e:
        print('Cannot connect to database: %s' % e)
        raise SystemExit

    useyear     = False # Use a single year or all years
    verbose     = CONFIG.getboolean('debug', 'verbose')
    chadwick    = os.path.abspath(CONFIG.get('chadwick', 'directory'))
    path        = CONFIG.get('path', 'download_path')
    csvpath     = '%s/csv' % path
    fnames      = []
    years       = []
    bound_param = '?' if CONFIG.get('database', 'engine') == 'sqlite' else '%s'

    os.chdir(path) # Chadwick seems to need to be in the directory

    parse_teamids('teams.csv', conn, bound_param)

    conn.close()


cli.add_command(retro)
cli.add_command(people)
cli.add_command(players)
cli.add_command(hist_players)
cli.add_command(teams)
