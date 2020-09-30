"""CLI tools for parsing downloaded data into the Retrosheet database. As we
will be calling this may times, potentially when the database already exists,
we need to make sure it can handle updating correctly."""

import os
import subprocess
import glob
import re
import datetime

import csv
import click

from ..utils import connect, load_modified_config


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

        sql = """
            SELECT
                *
            FROM
                rosters
            WHERE
                year = %s
            AND
                player_id = %s
            AND
                team_tx = %s
        """
        res = conn.execute(sql, [row[0], row[1], row[6]])

        # Avoid adding existing data.
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
        # Avoid adding existing data.
        conn.execute('DELETE FROM games WHERE game_id LIKE \'%%' + year + '%%\'')

        # In order to get access to the cpy_expert methods, we have to create
        # a raw database connection.
        fake_conn = conn.engine.raw_connection()
        fake_cur = fake_conn.cursor()
        with open(fname, 'r') as f_old, open(fname + '.new', 'w') as f_new:
            reader = csv.reader(f_old)
            writer = csv.writer(f_new)

            headers = reader.__next__()
            headers += ['inserted_time', 'uncertainty', 'source']
            writer.writerow(headers)

            for row in reader:
                row += [datetime.datetime.utcnow(), 1, 'Retrosheet']
                writer.writerow(row)

        with open(fname + '.new', 'rb') as f:
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

            # Avoid adding existing data.
            if res.rowcount == 1:
                continue

            headers += ['inserted_time', 'uncertainty', 'source']
            row += [datetime.datetime.utcnow(), 1, 'Retrosheet']

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
        # Avoid adding existing data.
        conn.execute('DELETE FROM events WHERE game_id LIKE \'%%' + year + '%%\'')

        # In order to get access to the cpy_expert methods, we have to create
        # a raw database connection.
        fake_conn = conn.engine.raw_connection()
        fake_cur = fake_conn.cursor()

        with open(fname, 'r') as f_old, open(fname + '.new', 'w') as f_new:
            reader = csv.reader(f_old)
            writer = csv.writer(f_new)

            headers = reader.__next__()
            headers += ['true_time', 'inserted_time', 'uncertainty', 'source']
            writer.writerow(headers)

            for row in reader:
                row += [None, datetime.datetime.utcnow(), 1, 'Retrosheet']
                writer.writerow(row)

        with open(fname + '.new', 'rb') as f:
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

            # Avoid adding existing data.
            if res.rowcount == 1:
                return True

            headers += ['true_time', 'inserted_time', 'uncertainty', 'source']
            row += [None, datetime.datetime.utcnow(), 1, 'Retrosheet']

            sql = 'INSERT INTO events(%s) VALUES(%s)' % (','.join(headers), ','.join([bound_param] * len(headers)))
            conn.execute(sql, row)


@click.group(help="CLI for parsing downloaded files into the database.")
def parse():
    pass


@click.command(help="Parse the Retrosheet downloads.")
def retro():
    try:
        conn = connect(CONFIG)
    except Exception as e:
        print('Cannot connect to database: %s' % e)
        raise SystemExit

    useyear = False  # Use a single year or all years
    verbose = CONFIG['debug']['verbose']
    chadwick = os.path.abspath(CONFIG['chadwick']['directory'])
    path = CONFIG['path']['download_path']
    csvpath = '%s/csv' % path
    fnames = []
    years = []
    bound_param = '?' if CONFIG['database']['engine'] == 'sqlite' else '%s'
    modules = ['teams', 'rosters', 'events', 'games']  # items to process

    if not os.path.exists(chadwick) \
        or not os.path.exists('%s/cwtools/cwevent' % chadwick) \
        or not os.path.exists('%s/cwtools/cwgame' % chadwick):
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
            cmd = "LD_LIBRARY_PATH=%s/cwlib/.libs %s/cwevent -q -n -f 0-96 -x 0-62 -y %d %d*.EV* > %s/events-%d.csv" % (chadwick, chadwick, year, year, csvpath, year)
            if(verbose):
                print("calling '" + cmd + "'")
            subprocess.call(cmd, shell=True)

        if not os.path.isfile('%s/games-%d.csv' % (csvpath, year)):
            cmd = "LD_LIBRARY_PATH=%s/cwlib/.libs %s/cwgame -q -n -f 0-83 -y %d %d*.EV* > %s/games-%d.csv" % (chadwick, chadwick, year, year, csvpath, year)
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


parse.add_command(retro)
