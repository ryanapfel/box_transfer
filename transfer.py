from ast import Raise
import configparser
from email.policy import default
import click
from src.FileTransfer import FileTransfer
import sqlite3 as sql
import os
from pathlib import Path
import pandas as pd
import json

# ESTABLISH DEFAULTS
config = configparser.ConfigParser()
relativePath = os.path.dirname(os.path.abspath(__file__))
config.read(os.path.join(relativePath, "config.cfg"))

LOG_PATH = os.path.join(config["logs"]["log_destination"])
LOG_DEST = os.path.join(LOG_PATH, config["logs"]["log_file_name"])

VERBOSE = config["logs"]["verbose"]


@click.group()
def cli():
    pass


def createDataFolder():
    dataPath = os.path.join(relativePath, "data")
    if not os.path.isdir(dataPath):
        os.mkdir(dataPath)
    return dataPath


def initDB(path):
    path = Path(path)

    if not path.is_file():
        print("DB not created")
        with open(path, "w") as fp:
            pass
        print("Created DB")

    try:
        with sql.connect(path) as conn:
            cursor = conn.cursor()

            cursor.execute(
                """CREATE TABLE uploads
                        (date date, study text,  root_dir text, file text)"""
            )
            cursor.execute(
                """Create Table errors
                            (date date, study text, error text)"""
            )

            conn.commit()
        print("Tables initialized inside db")
    except Exception as e:
        print(e)


def getDatabasePath():
    dataPath = createDataFolder()
    dbpath = os.path.join(dataPath, "uploads.sql")
    if not os.path.isfile(dbpath):
        initDB(dbpath)
    return dbpath


def getStudies():
    studiesPath = os.path.join(relativePath, "data/studies.json")
    with open(studiesPath) as f:
        studies = json.load(f)

    return studies


def getDestinations():
    studiesPath = os.path.join(relativePath, "data/destinations.json")
    with open(studiesPath) as f:
        studies = json.load(f)
    return studies


@cli.command(help="Add study locations and there corresponding cloud folders")
@click.option("--study", prompt="Enter the study name", type=str)
@click.option(
    "--spath",
    prompt="Enter the path to where files are located for this study",
    type=click.Path(exists=True),
)
def addstudy(study, spath):
    dataPath = createDataFolder()

    defaultStudyPath = os.path.join(dataPath, "studies.json")
    if not os.path.isfile(defaultStudyPath):
        with open(defaultStudyPath, "w+") as f:
            json.dump({}, f, indent=2)
            print(f"Initializing a new StudyFile at {defaultStudyPath}")

    with open(defaultStudyPath) as f:
        studies = json.load(f)
    if study in studies.keys():
        raise ValueError("Study Path Already Exists. New study path was not added")

    if spath in studies.values():
        raise ValueError("Path is already in destinations")
    studies[study] = spath

    with open(defaultStudyPath, "w") as f:
        json.dump(studies, f, indent=2)
        print(f"Added {study} into study locations")


@cli.command(help="Add destinations")
@click.option("--name", prompt="Enter the transfer destination database name", type=str)
@click.option(
    "--spath",
    prompt="Enter the path to where files should be transfered",
    type=click.Path(exists=True),
)
def adddestination(name, spath):
    dataPath = createDataFolder()

    defaultStudyPath = os.path.join(dataPath, "destinations.json")
    if not os.path.isfile(defaultStudyPath):
        with open(defaultStudyPath, "w") as f:
            json.dump({}, f, indent=2)
            print(f"Initializing a new destination file at {defaultStudyPath}")

    with open(defaultStudyPath) as f:
        dests = json.load(f)
    if name in dests.keys():
        raise ValueError("Transfer destination already exists")

    if spath in dests.values():
        raise ValueError("Path is already in destinations")

    dests[name] = spath

    with open(defaultStudyPath, "w") as f:
        json.dump(dests, f, indent=2)
        print(f"Added {name} into transfer destinations")


@cli.command(help="List all studies that have been transfered")
@click.option("--range", default="day", help="timeframe to query succesful transfers")
def ls(range):
    dbPath = getDatabasePath()
    conn = sql.connect(dbPath)
    if range == "day":
        query = """SELECT study, root_dir, file FROM uploads WHERE date BETWEEN datetime('now', 'start of day') AND datetime('now', 'localtime');"""
    print(pd.read_sql(query, conn).to_string())
    conn.close()


@cli.command(help="List all possible studies that can be transfered")
def studies():
    studies = getStudies()
    for s in studies:
        print(s)


@cli.command(help="Transfer files from study folder to Horos DB folder")
@click.option("--study", default=None)
def transfer(study):
    studies = getStudies()
    dbPath = getDatabasePath()
    for name, path in getDestinations().items():
        print(f"Tranfering files to {name} at {path}")
        ft = FileTransfer(dbPath, studies, path)
        if study == None:
            ft.transfer()
        else:
            ft.transfer(study)
        ft.create_log(LOG_DEST)


@cli.command(help="Fill records without copying files")
@click.option("--study", default=None)
def fill(study):
    print(f"Filling files")
    studies = getStudies()
    dbPath = getDatabasePath()
    ft = FileTransfer(dbPath, studies, "")
    if study == None:
        ft.fillDataBase()
    else:
        ft.fillDataBase(study)

        ft.create_log(LOG_DEST)


@cli.command(help="Export master log")
def log():
    studies = getStudies()
    dbPath = getDatabasePath()
    ft = FileTransfer(dbPath, studies, "")
    ft.master_log(LOG_DEST)
    click.echo(f"Master Log Exported to: {LOG_DEST}")


if __name__ == "__main__":
    cli()
