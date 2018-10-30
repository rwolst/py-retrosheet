"""Command line tools for downloading necessary files for the Retrosheet
database. As these files may regularly change we may have a system or CRON
job set up to refresh them at regular intervals."""

import urllib.request as urllib
import os
import configparser as ConfigParser
import queue as Queue
import re
import click
import wget

from classes.fetcher import Fetcher

# load configs
CONFIG = ConfigParser.ConfigParser()
CONFIG.readfp(open('config.ini'))

# load and evaluate download directory
PATH = CONFIG.get('download', 'directory')
ABSOLUTE_PATH = os.path.abspath(PATH)

# test for existence of download directory
# create if does not exist
try:
    os.chdir(ABSOLUTE_PATH)
except OSError:
    print("Directory %s does not exist, creating..." % ABSOLUTE_PATH)
    os.makedirs(ABSOLUTE_PATH)

# load settings into separate var
# can this be replaced by config var in the future?
OPTIONS = {}
OPTIONS['verbose'] = CONFIG.getboolean('debug', 'verbose')


def remove_file(fname):
    """Attempts to remove any existing file with the given fname in our
    directory path."""
    try:
        os.remove(ABSOLUTE_PATH + '/' + fname)
        print("Removed existing %s file." % fname)
    except OSError:
        print("No existing %s file to remove." % fname)


@click.group(help="Download files for populating Retrosheet database.")
def cli():
    pass


@click.command(help="Download Retrosheet years.")
@click.option('--years', '-y', multiple=True, default=['2012'], type=str, help="The years we want to download.")
def retro(years):
    """Download Retrosheet files for the given years."""
    # initialize variables / set defaults
    queue = Queue.Queue()
    threads = []
    num_threads = CONFIG.getint('download', 'num_threads')

    ##################################
    # Queue Event Files for Download #
    ##################################

    if CONFIG.getboolean('download', 'dl_eventfiles'):
        # log next action
        print("Queuing up Event Files for download.")

        # parse retrosheet page for files and add urls to the queue
        retrosheet_url = CONFIG.get('retrosheet', 'eventfiles_url')
        pattern = r'(\d{4}?)eve\.zip'
        html = urllib.urlopen(retrosheet_url).read()
        matches = re.finditer(pattern, str(html), re.S)
        for match in matches:
            # if we are looking for a year and this isnt the one, skip it
            if match.group(1) not in years:
                continue

            # compile absolute url and add to queue
            url = 'http://www.retrosheet.org/events/%seve.zip' % match.group(1)
            queue.put(url)

    #################################
    # Queue Game Logs for Download #
    #################################

    if CONFIG.getboolean('download', 'dl_gamelogs'):
        # log next action
        print("Queuing up Game Logs for download.")

        # parse retrosheet page for files and add urls to the queue
        retrosheet_url = CONFIG.get('retrosheet', 'gamelogs_url')
        pattern = r'gl(\d{4})\.zip'
        html = urllib.urlopen(retrosheet_url).read()
        matches = re.finditer(pattern, html, re.S)
        for match in matches:
            # if we are looking for a year and this isnt the one, skip it
            if match.group(1) not in years:
                continue

            # compile absolute url and add to queue
            url = 'http://www.retrosheet.org/gamelogs/gl%s.zip' % match.group(1)
            queue.put(url)

    ##################
    # Download Files #
    ##################
    # spin up threads
    for i in range(num_threads):
        t = Fetcher(queue, ABSOLUTE_PATH, OPTIONS)
        t.start()
        threads.append(t)

    # wait for all threads to finish
    for thread in threads:
        thread.join()


@click.command(help="Download PeopleIDs.")
def people():
    # Remove file if already exists.
    remove_file('people.csv')

    wget.download('https://raw.githubusercontent.com/chadwickbureau/register/master/data/people.csv',
                  out=ABSOLUTE_PATH + '/people.csv')
    print()


@click.command(help="Download PlayerIDs.")
def players():
    # Remove file if already exists.
    remove_file('players.csv')
    remove_file('hist_players.csv')

    wget.download('http://crunchtimebaseball.com/master.csv',
                  out=ABSOLUTE_PATH + '/players.csv')
    print()
    wget.download('https://raw.githubusercontent.com/chadwickbureau/baseballdatabank/master/core/People.csv',
                  out=ABSOLUTE_PATH + '/hist_players.csv')
    print()


@click.command(help="Download TeamIDs.")
def teams():
    remove_file('teams.csv')

    wget.download('http://www.retrosheet.org/CurrentNames.csv',
                  out=ABSOLUTE_PATH + '/teams.csv')
    print()


cli.add_command(retro)
cli.add_command(people)
cli.add_command(players)
cli.add_command(teams)


if __name__ == "__main__":
    cli()
