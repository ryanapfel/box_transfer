import configparser
import json
import os
import sqlite3 as sql
from pathlib import Path

import click
import pandas as pd

from .cloud_transfer import (
    DicomProcessTransfer,
    FileTransfer,
    get_db_path,
    get_studies,
    init_db,
    reset_database,
)
from .proccess import Process


@click.group()
def cli():
    pass


@cli.command(help="Initialize database")
def init():
    init_db()


@cli.command(help="Add study locations with input and output paths")
@click.option("--study", prompt="Enter the study name", type=str)
@click.option(
    "--input-path",
    prompt="Enter the input path (must exist)",
    type=click.Path(),
)
@click.option(
    "--output-path",
    prompt="Enter the output path (will be created if it doesn't exist)",
    type=click.Path(),
)
def addstudy(study, input_path, output_path):
    db_path = get_db_path()

    # Sanitize paths by removing quotes or any extra spaces
    input_path = Path(input_path.strip().strip("'").strip('"'))
    output_path = Path(output_path.strip().strip("'").strip('"'))

    # Check if input path exists
    if not input_path.exists():
        print(f"Input path '{input_path}' does not exist. Please provide a valid path.")
        return

    # Ensure output path exists or create it
    if not output_path.exists():
        print(f"Output path '{output_path}' does not exist. Creating it now.")
        output_path.mkdir(parents=True)

    try:
        with sql.connect(db_path) as conn:
            cursor = conn.cursor()

            # Check if the study name already exists
            cursor.execute("SELECT * FROM studies WHERE study_name = ?", (study,))
            existing_study = cursor.fetchone()

            if existing_study:
                raise ValueError(
                    "Study name already exists. New study path was not added."
                )

            # Insert the study with input and output paths into the database
            cursor.execute(
                "INSERT INTO studies (study_name, input_path, output_path) VALUES (?, ?, ?)",
                (study, str(input_path), str(output_path)),
            )
            conn.commit()

            print(
                f"Added '{study}' with input path '{input_path}' and output path '{output_path}' into the studies table."
            )

    except sql.Error as e:
        print(f"An error occurred: {e}")


@cli.command(help="List all studies with their input and output paths")
def studies():
    # Retrieve all studies from the database
    studies = get_studies()

    if studies:
        print("List of studies:")
        for study_name, paths in studies.items():
            input_path = paths["input_path"]
            output_path = paths["output_path"]
            print(f"Study: {study_name}")
            print(f"  Input Path: {input_path}")
            print(f"  Output Path: {output_path}")
            print("-" * 40)  # Divider for better readability


@cli.command(help="Transfer files from study folder to Horos DB folder")
@click.option("--study", default=None)
def transfer(study):
    studies = get_studies()
    dbPath = init_db()
    ft = DicomProcessTransfer(dbPath, studies, "")
    if study == None:
        ft.transfer()
    else:
        ft.transfer(study)

    ft.create_log()


@cli.command(help="Reset upload records for a study")
@click.option("--study", default=None)
def reset(study):
    studies = get_studies()
    dbPath = get_db_path()

    reset_database(dbPath, studies)


@cli.command(help="Delete the SQLite database")
def delete_database():
    """
    Delete the SQLite database file.
    """
    db_path = get_db_path()

    # Check if the database exists
    if Path(db_path).exists():
        # Confirm deletion with the user
        confirm = input(
            f"Are you sure you want to delete the database at {db_path}? (y/n): "
        ).lower()
        if confirm == "y":
            try:
                os.remove(db_path)
                print(f"Database at {db_path} has been deleted.")
            except Exception as e:
                print(f"Error deleting database: {e}")
        else:
            print("Database deletion aborted.")
    else:
        print(f"No database found at {db_path}.")


@cli.command(help="Add directory to clean in as first arg")
@click.argument("path", type=click.Path(exists=True))
def zip(path):
    proccess = Process(path)
    proccess.process_dir()


@cli.command(help="Show the database path")
def show_db_path():
    db_path = get_db_path()
    print(f"Database path: {db_path}")


if __name__ == "__main__":
    cli()
