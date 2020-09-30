"""Command line tools for downloading necessary files for the Retrosheet
database. As these files may regularly change we may have a system or CRON
job set up to refresh them at regular intervals."""

import urllib.request as urllib
import os
import queue as Queue
import re
import click
import wget

from ..classes.fetcher import Fetcher
from ..utils import load_modified_config


# Load CONFIG.
CONFIG = load_modified_config()

# Load and evaluate download directory
PATH = CONFIG['download']['directory']
if CONFIG['download']['use_tmp']:
    ABSOLUTE_PATH = '/tmp/' + PATH
else:
    ABSOLUTE_PATH = os.path.abspath(PATH)

# Test for existence of download directory
# Create if does not exist
try:
    os.chdir(ABSOLUTE_PATH)
except OSError:
    print("Directory %s does not exist, creating..." % ABSOLUTE_PATH)
    os.makedirs(ABSOLUTE_PATH)

# load settings into separate var
# can this be replaced by config var in the future?
OPTIONS = {}
OPTIONS['verbose'] = CONFIG['debug']['verbose']


def remove_file(fname):
    """Attempts to remove any existing file with the given fname in our
    directory path."""
    try:
        os.remove(ABSOLUTE_PATH + '/' + fname)
        print("Removed existing %s file." % fname)
    except OSError:
        print("No existing %s file to remove." % fname)


@click.group(help="Download files for populating Retrosheet database.")
def download():
    pass


@click.command(help="Download Retrosheet years.")
@click.argument('years', nargs=-1, type=str)
def retro(years):
    """Download Retrosheet files for the given years."""
    # initialize variables / set defaults
    queue = Queue.Queue()
    threads = []
    num_threads = CONFIG['download']['num_threads']

    # Queue event files.
    if CONFIG['download']['dl_eventfiles']:
        # log next action
        print("Queuing up Event Files for download.")

        # parse retrosheet page for files and add urls to the queue
        retrosheet_url = CONFIG['retrosheet']['eventfiles_url']
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

    # Queue game files.
    if CONFIG['download']['dl_gamelogs']:
        # log next action
        print("Queuing up Game Logs for download.")

        # parse retrosheet page for files and add urls to the queue
        retrosheet_url = CONFIG['retrosheet']['gamelogs_url']
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

    # Download.
    # spin up threads
    for i in range(num_threads):
        t = Fetcher(queue, ABSOLUTE_PATH, OPTIONS)
        t.start()
        threads.append(t)

    # wait for all threads to finish
    for thread in threads:
        thread.join()
    print("\nSaved file to %s" % ABSOLUTE_PATH)


download.add_command(retro)
