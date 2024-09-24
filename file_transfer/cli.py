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
@click.option(
    "--study",
    default=None,
    help="Specify the study to transfer. If not provided, all studies are transferred.",
)
@click.option(
    "--retry-errors",
    is_flag=True,
    help="Retry transferring files that previously encountered errors.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Simulate the transfer without actually transferring files.",
)
@click.option(
    "--force",
    is_flag=True,
    help="Force transfer of files even if they have already been transferred.",
)
def transfer(study, retry_errors, dry_run, force):
    studies = get_studies()
    dbPath = init_db()

    # Initialize DicomProcessTransfer with the provided database path and studies
    ft = DicomProcessTransfer(dbPath, studies, "")

    # If no study is provided, process all studies, else process the specific study
    if study is None:
        ft.transfer(retry_errors=retry_errors, dry_run=dry_run, force=force)
    else:
        ft.transfer(study, retry_errors=retry_errors, dry_run=dry_run, force=force)

    # Create the log at the end of the transfer
    ft.create_log()


@cli.command(help="Transfer files from a specific subdirectory of the input path")
@click.option("--study", required=True, help="Specify the study to transfer.")
@click.option(
    "--directory",
    required=True,
    help="Specify the subdirectory inside the input path.",
)
@click.option(
    "--retry-errors",
    is_flag=True,
    help="Retry transferring files that previously encountered errors.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Simulate the transfer without actually transferring files.",
)
@click.option(
    "--force",
    is_flag=True,
    help="Force transfer of files even if they have already been transferred.",
)
def transfer_sub(study, directory, retry_errors, dry_run, force):
    studies = get_studies()
    dbPath = init_db()

    ft = DicomProcessTransfer(dbPath, studies, "")

    ft.transfer_subdirectory(
        study, directory, retry_errors=retry_errors, dry_run=dry_run, force=force
    )
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


@cli.command(help="Delete all contents of the destination folder")
@click.option("--study", required=True, help="Specify the study whose destination folder you want to clear.")
def delete_destination(study):
    studies = get_studies()
    db_path = init_db()

    # Check if the study exists
    if study not in studies:
        print(f"Study '{study}' not found in the database.")
        return

    # Get the output path of the specified study
    output_path = studies[study]["output_path"]

    # Initialize the transfer object and delete the destination
    ft = DicomProcessTransfer(db_path, studies, "")
    ft.delete_destination(output_path)

    print(f"All contents of the destination folder '{output_path}' have been deleted.")


if __name__ == "__main__":
    cli()
