PY-RETROSHEET
=============

Python scripts for Retrosheet data downloading and parsing.

YE REQUIREMENTS
---------------

- Chadwick 0.6.2 http://chadwick.sourceforge.net/

- python 2.5+ (don't know about 3.0, sorry)

- sqlalchemy: http://www.sqlalchemy.org/

- [if using postgres] psycopg2 python package (dependency for sqlalchemy)

USAGE
-----

### Setup

    cp conf/config.ini.dist conf/config.ini

Edit `conf/config.ini` as needed.  See the steps below for what might need to be changed.

### Download

    python download.py [-y <4-digit-year> | --year <4-digit-year>]

The `scripts/download.py` script downloads Retrosheet data. Edit the config.ini file to configure what types of files should be downloaded. Optionally set the year to download via the command line argument.

- `download` > `dl_eventfiles` determines if Retrosheet Event Files should be downloaded or not. These are the only files that can be processed by `parse.py` at this time.

- `download` > `dl_gamelogs` determines if Retrosheet Game Logs should be downloaded or not. These are not able to be processed by `parse.py` at this time.

### Parse into SQL

    python parse.py [-y <4-digit-year>]

After the files have been downloaded, parse them into SQL with `parse.py`.

1. Create database called `retrosheet` (or whatever).

2. Add schema to the database w/ the included SQL script (the .postgres.sql one works nicely w/ PG, the other w/ MySQL)

3. Configure the file `config.ini` with your appropriate `ENGINE`, `USER`, `HOST`, `PASSWORD`, and `DATABASE` values - if you're using postgres, you can optionally define `SCHEMA` and download directory

    - Valid values for `ENGINE` are valid sqlalchemy engines e.g. 'mysql', 'postgresql', or 'sqlite',

    - If you have your server configured to allow passwordless connections, you don't need to define `USER` and `PASSWORD`.

    - If you are using sqlite3, `database` in the config should be the path to your database file.

    - Specify directory for retrosheet files to be downloaded to, needs to exist before script runs

5. Run `parse.py` to parse the files and insert the data into the database. (optionally use `-y YYYY` to import just one year)

#### Environment Variables (optional)

Instead of editing the `config.ini` file, you may, optionally, use environment variables to set configuration options. Name the environment variables in the format `<SECTION>_<OPTION>`. Thus, an environment variable that sets the database username would be called `DATABASE_USER`. The environment variables overwrite any settings in the `config.ini` file.

Example,

    $ DATABASE_DATABASE=rtrsht_testing CHADWICK_DIRECTORY=/usr/bin/ python parse.py -y 1956


YE GRATITUDE
------------

Github user jeffcrow made many fixes and additions and added sqlite support

JUST THE DATA
-------------

If you're using PostgreSQL (and you should be), you can get a dump of all data up through 2016 (warning: 521MB) [here](https://www.dropbox.com/s/kg01np4ev3u2jsx/retrosheet.2016.psql?dl=0)

### Importing into PostgreSQL
After creating a PostgreSQL user named `wells`, you can create a database from the dump by running `pg_restore -U <USERNAME> -d <DATABASE> -1 retrosheet.2016.psql`.

### License
I don't care. Have at it.

### Parse Test
To make sure the parsing updating is working correctly to the following:

1. Parse all players and people into the database.

2. Remove one player from peopleIDs, re-parse and make sure it has been re-added.

    - DELETE FROM peopleids WHERE key_uuid = '663ecca1-6b4e-4a11-9494-1caa6d6b2d13';

    - SELECT COUNT(*) FROM peopleids;

    - pyretro_parse people

    - SELECT COUNT(*) FROM peopleids;

3. Remove one player from playerIDs, re-parse and make sure it has been re-added.

    - DELETE FROM playerIDS WHERE mlb_id = '592091';

    - SELECT COUNT(*) FROM playerids;

    - pyretro_parse players

    - SELECT COUNT(*) FROM playerids;

4. Remove one player from hist_playerIDs, re-parse and make sure it has been re-added.

    - DELETE FROM hist_playerIDS WHERE playerid = 'aardsda01';

    - SELECT COUNT(*) FROM hist_playerIDs;

    - pyretro_parse hist-players

    - SELECT COUNT(*) FROM hist_playerIDs;

5. Remove one player from teamIDs, re-parse and make sure it has been re-added.

    - DELETE FROM teamIDs WHERE id_current = 'ANA';

    - SELECT COUNT(*) FROM teamIDs;

    - pyretro_parse teams

    - SELECT COUNT(*) FROM teamIDs;

6. Update a player to 'ravenholm-...' in playerIDs, who has a corresponding
   entry in peopleIDs. Parse people again and make sure it has been updated
   to the correct retroID in playerIDs, events and games.

    - UPDATE playerIDs SET retro_id = 'ravenholm-112526' WHERE retro_id = 'colob001';

    - UPDATE events SET pit_id = 'ravenholm-112526' WHERE pit_id = 'colob001';

    - UPDATE games SET home_start_pit_id = 'ravenholm-112526' WHERE home_start_pit_id = 'colob001';

    - UPDATE games SET away_start_pit_id = 'ravenholm-112526' WHERE away_start_pit_id = 'colob001';

    - SELECT COUNT(*), retro_id FROM playerIDs WHERE retro_id in ('ravenholm-112526', 'colob001') GROUP BY retro_id;

    - SELECT COUNT(*), pit_id FROM events WHERE pit_id in ('ravenholm-112526', 'colob001') GROUP BY pit_id;

    - SELECT COUNT(*), home_start_pit_id FROM games WHERE home_start_pit_id in ('ravenholm-112526', 'colob001') GROUP BY home_start_pit_id;

    - SELECT COUNT(*), away_start_pit_id FROM games WHERE away_start_pit_id in ('ravenholm-112526', 'colob001') GROUP BY away_start_pit_id;

    - pyretro_parse people

    - SELECT COUNT(*), retro_id FROM playerIDs WHERE retro_id in ('ravenholm-112526', 'colob001') GROUP BY retro_id;

    - SELECT COUNT(*), pit_id FROM events WHERE pit_id in ('ravenholm-112526', 'colob001') GROUP BY pit_id;

    - SELECT COUNT(*), home_start_pit_id FROM games WHERE home_start_pit_id in ('ravenholm-112526', 'colob001') GROUP BY home_start_pit_id;

    - SELECT COUNT(*), away_start_pit_id FROM games WHERE away_start_pit_id in ('ravenholm-112526', 'colob001') GROUP BY away_start_pit_id;

7. Update a player to 'ravenholm-...' in playerIDs. Parse players again and
   make sure it has been updated to the correct retroID in playerIDs, events
   and games.

    - UPDATE playerIDs SET retro_id = 'ravenholm-112526' WHERE retro_id = 'colob001';

    - UPDATE events SET pit_id = 'ravenholm-112526' WHERE pit_id = 'colob001';

    - UPDATE games SET home_start_pit_id = 'ravenholm-112526' WHERE home_start_pit_id = 'colob001';

    - UPDATE games SET away_start_pit_id = 'ravenholm-112526' WHERE away_start_pit_id = 'colob001';

    - SELECT COUNT(*), retro_id FROM playerIDs WHERE retro_id in ('ravenholm-112526', 'colob001') GROUP BY retro_id;

    - SELECT COUNT(*), pit_id FROM events WHERE pit_id in ('ravenholm-112526', 'colob001') GROUP BY pit_id;

    - SELECT COUNT(*), home_start_pit_id FROM games WHERE home_start_pit_id in ('ravenholm-112526', 'colob001') GROUP BY home_start_pit_id;

    - SELECT COUNT(*), away_start_pit_id FROM games WHERE away_start_pit_id in ('ravenholm-112526', 'colob001') GROUP BY away_start_pit_id;

    - pyretro_parse players

    - SELECT COUNT(*), retro_id FROM playerIDs WHERE retro_id in ('ravenholm-112526', 'colob001') GROUP BY retro_id;

    - SELECT COUNT(*), pit_id FROM events WHERE pit_id in ('ravenholm-112526', 'colob001') GROUP BY pit_id;

    - SELECT COUNT(*), home_start_pit_id FROM games WHERE home_start_pit_id in ('ravenholm-112526', 'colob001') GROUP BY home_start_pit_id;

    - SELECT COUNT(*), away_start_pit_id FROM games WHERE away_start_pit_id in ('ravenholm-112526', 'colob001') GROUP BY away_start_pit_id;
