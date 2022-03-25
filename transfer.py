
import configparser
from email.policy import default
import click
from src.FileTransfer import FileTransfer
import sqlite3 as sql
import os
from pathlib import Path
import pandas as pd


# ESTABLISH DEFAULTS
config = configparser.ConfigParser()
relativePath = os.path.dirname(os.path.abspath(__file__))
config.read(f'{relativePath}/config.cfg')
USER = os.path.expanduser('~')

DBPATH = config['database']['database_path']
LOG_FILE_NAME = config['logs']['log_file_name']
LOG_PATH = USER + config['logs']['log_destination']
LOG_DEST = LOG_PATH + LOG_FILE_NAME


VERBOSE = config['logs']['verbose']
DESTDIR = config['destination']['destination']
ROOT_DIRS = {k: USER + v for k, v in dict(config['sourcePaths']).items()}


@click.group()
def cli():
    pass


@cli.command(help='initialize db if not already')
@click.option('--path', default=DBPATH)
def initdb(path):
    path = Path(path)
    if not path.is_file():
        print("DB not created")
        with open(path, 'w') as fp:
            pass
        print("Created DB")
    else:
        print("DB already created")
    try:
        with sql.connect(path) as conn:
            cursor = conn.cursor()

            cursor.execute('''CREATE TABLE uploads
                        (date date, study text,  root_dir text, file text)''')
            cursor.execute('''Create Table errors
                            (date date, study text, error text)''')

            conn.commit()
        print("Table initialized inside db")
    except Exception as e:
        print(e)
    

@cli.command(help='List all studies that have been transfered')
@click.option('--range', default='day', help='timeframe to query succesful transfers')
@click.option('--path', default=DBPATH)
def ls(range, path):
    conn = sql.connect(path)
    if range == 'day':
        query = """SELECT * FROM uploads WHERE date BETWEEN datetime('now', 'start of day') AND datetime('now', 'localtime');"""
    print(pd.read_sql(query, conn))
    conn.close()

    





@cli.command(help='List all possible studies that can be transfered')
def studies():
    for s in ROOT_DIRS:
        print(s)


@cli.command(help='Transfer files from study folder to Horos DB folder')
@click.option('--study', default='')
@click.option('--log_path', default=LOG_PATH)
@click.option('--log_file_name', default=LOG_FILE_NAME)
def transfer(study, log_path, log_file_name, verbose=True):
    fn = log_path + log_file_name
    ft = FileTransfer(DBPATH, ROOT_DIRS, DESTDIR, verbose=verbose)
    ft.transfer(study)
    ft.create_log(fn)


@cli.command(help='Fill records without copying files')
@click.option('--study', default=None)
@click.option('--log_path', default=LOG_PATH)
@click.option('--log_file_name', default=LOG_FILE_NAME)
def fill(study, log_path, log_file_name):
    fn = log_path + log_file_name
    ft = FileTransfer(DBPATH, ROOT_DIRS, DESTDIR)
    if study == None:
        ft.fillDataBase()
    else:
        ft.fillDataBase(study)

    ft.create_log(fn)


@cli.command(help='Export master log')
def log():
    ft = FileTransfer(DBPATH, ROOT_DIRS, DESTDIR)
    ft.master_log(LOG_DEST)
    click.echo(f"Master Log Exported to: {LOG_DEST}")


if __name__ == '__main__':
    cli()
