"""
Main file implementing logic for the CLI.

The command-line interface is implemented using
Click (https://click.palletsprojects.com), and is a simple high-level API for
the main methods inside pypgsync.py itself.
"""

import click
import crayons
import time
import sys

from pypgsync import pypgsync
from pypgsync.utils import create_spinner

# const: chunksize hard-limit allowed by the cli
MAX_CHUNKSIZE = 10000


def validate_chunk_size(ctx, param, value):
    if value <= MAX_CHUNKSIZE:
        raise click.BadParameter(
            'Chunksize must be lower or equal to {}'.format(MAX_CHUNKSIZE))


@click.group()
@click.version_option()
def cli():
    """This script syncs data from a source PostgreSQL table into a destination
    table with the same structure. Both tables must be in the same PostgreSQL
    server. If the destination table does not exist, the script will create it.
    It also expects both tables to share the same name.
    """


@cli.command(short_help="Start sync in single-time mode.")
@click.option('-h', '--hostname', required=True, default="localhost", type=str,
              help="Hostname", show_default=True)
@click.option('-p', '--port', required=True, default=5432, type=int,
              help="Port", show_default=True)
@click.option('-c', '--chunksize', required=True, default=MAX_CHUNKSIZE,
              type=int, help="Transaction chunk size".format(MAX_CHUNKSIZE),
              show_default=True)
@click.argument('source_db')
@click.argument('destination_db')
@click.argument('tablename')
@click.argument('username')
@click.password_option()
def single(hostname, port, source_db, destination_db, tablename,
           username, password, chunksize):
    """Single-time mode will sync table data, loading data up until the time
    execution has started, using the updated_at column as a reference point.
    After this criteria is met, the script will exit.
    """

    click.secho(
        str(crayons.white('Starting single-time mode… ᕙ(⇀‸↼‶)ᕗ', bold=True)))

    sync(hostname, port, source_db, destination_db, tablename,
         username, password, chunksize)


@cli.command(short_help="Run in continous mode")
@click.option('-h', '--hostname', required=True, default="localhost", type=str,
              help="Hostname", show_default=True)
@click.option('-p', '--port', required=True, default=5432, type=int,
              help="Port", show_default=True)
@click.option('-c', '--chunksize', required=True, default=MAX_CHUNKSIZE,
              type=int, help="Transaction chunk size".format(MAX_CHUNKSIZE),
              show_default=True)
@click.option('-d', '--delay', default=5, type=int,
              help="Time in seconds to wait between executions",
              show_default=True)
@click.argument('source_db')
@click.argument('destination_db')
@click.argument('tablename')
@click.argument('username')
@click.password_option()
def continuous(hostname, port, source_db, destination_db, tablename,
               username, password, chunksize, delay):
    """Continuous mode basically executes the same algorithm for single-time
    mode, but continuously repeating in order to keep the two tables in sync,
    waiting `delay` seconds between each run.
    """

    click.secho(
        str(crayons.white('Starting continuous mode… ᕙ(⇀‸↼‶)ᕗ', bold=True)))

    try:
        while True:
            sync(hostname, port, source_db, destination_db, tablename,
                 username, password, chunksize)
            time.sleep(delay)
    except (SystemExit, KeyboardInterrupt):
        # already handled by the sync method, just make sure we always exit
        sys.exit(1)


def sync(hostname, port, source_db, destination_db, tablename, username,
         password, chunksize):
    t0 = time.time()
    # colorize our spinner text
    spinner_text = str(crayons.green("{}…", bold=True))
    with create_spinner(text=spinner_text.format("Instancing")) as sp:
        try:
            # instance result iterator
            result_iter = pypgsync.start_single(
                host=hostname,
                port=port,
                src_db=source_db,
                dst_db=destination_db,
                tbl=tablename,
                user=username,
                passwd=password,
                chunksize=chunksize)

            total_rows = 0
            # loop through result in chunks
            for r in result_iter:
                total_rows += r[0]
                # variables used to calculate progress and ETA
                t1 = time.time()
                rows_per_sec = round(r[0] / (t1 - t0))
                bar_length = int((r[2] - r[0]) / r[2] * 25)
                bar_progress = int(r[0] / r[2] * 25)
                eta = int((t1 - t0) / (r[0] / r[2]) - (t1 - t0))

                # update our pretty spinner
                sp.text = (spinner_text.format("Syncing") + " ({} rows/s) | {}% ["
                           + "#" * bar_progress + "-" * bar_length
                           + "] ETA: {}s").format(rows_per_sec,
                                                  int(r[0] / r[2] * 100), eta)
            sp.stop()
            click.secho("{} rows synced. No rows left to sync!（ ^_^）o自自o（^_^ ）".format(total_rows),
                        fg="green", bold=True)
        except RuntimeError as e:
            sp.stop()
            click.secho('FATAL: {}'.format(e.args[0]), fg="red", bold=True)
            sys.exit(1)
        except (SystemExit, KeyboardInterrupt):
            sp.stop()
            click.secho('Stopping… (._.)', fg="red", bold=True)
            sys.exit(1)
