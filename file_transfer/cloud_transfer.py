import hashlib
import json
import logging
import os
import shutil
import sqlite3 as sql
from datetime import datetime
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from .proccess import Process

TOOL_NAME = "file_transfer"


def get_db_path():
    # Cross-platform way to get user's home directory
    home_dir = Path.home()

    # Create a directory under ~/.config/your_tool (Linux/macOS) or %APPDATA%\your_tool (Windows)
    config_dir = home_dir / ".config" / TOOL_NAME / ""

    if not config_dir.exists():
        config_dir.mkdir(parents=True)

    # Define the full path for the database file
    db_path = config_dir / "database.sql"

    return db_path


def init_db():
    db_path = get_db_path()  # Use your function to get the database path

    if not Path(db_path).exists():
        print(f"Creating new database at {db_path}")
        with sql.connect(db_path) as conn:
            cursor = conn.cursor()

            # Create the tables if they do not exist
            cursor.execute(
                """CREATE TABLE IF NOT EXISTS uploads
                    (date date, study text, root_dir text, file text, file_hash text)"""
            )
            cursor.execute(
                """CREATE TABLE IF NOT EXISTS errors
                        (date date, study text, error text)"""
            )
            cursor.execute(
                """CREATE TABLE IF NOT EXISTS studies
                        (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                         study_name TEXT UNIQUE, 
                         input_path TEXT, 
                         output_path TEXT)"""
            )

            conn.commit()
        print("Database and tables initialized.")

    return db_path


def reset_database(database_path, studies=None):
    """
    Reset (delete) upload records for specified studies or all studies if none are specified.

    Args:
        database_path (str): Path to the SQLite database.
        studies (list): List of study names to delete records for.
                        If None, all records will be deleted.
    """
    with sql.connect(database_path) as conn:
        cursor = conn.cursor()

        for study in studies:
            cursor.execute("DELETE FROM uploads WHERE study = ?", (study,))
            cursor.execute("DELETE FROM errors WHERE study = ?", (study,))
            print(f"Records for study {study} have been deleted.")

        conn.commit()


def get_studies():
    db_path = init_db()  # Ensure the DB is initialized and get the path

    try:
        with sql.connect(db_path) as conn:
            cursor = conn.cursor()

            # Fetch all studies from the `studies` table
            cursor.execute("SELECT study_name, input_path, output_path FROM studies")
            studies = cursor.fetchall()

            # Return a list of studies with input and output paths
            if not studies:
                print("No studies found.")
                return []

            studies_dict = {
                study_name: {"input_path": input_path, "output_path": output_path}
                for study_name, input_path, output_path in studies
            }
            return studies_dict

    except sql.Error as e:
        print(f"An error occurred: {e}")
        return []


class FileTransfer:
    """
    Handles the transfer of files from study directories to the destination,
    while tracking the progress and storing relevant information in a database.
    """

    def __init__(self, database_path, root_directories, verbose=True):
        """
        Initialize FileTransfer object.

        Args:
            database_path (str): Path to the SQLite database.
            root_directories (dict): A dictionary of study names and their root directories.
            temp_dir (str): The temporary directory to use during file transfers.
            verbose (bool): Print detailed transfer progress (default: False).
        """
        self.verbose = verbose
        self.database_path = database_path
        self.root_directories = root_directories  # Expected to be a dict with 'input_path' and 'output_path' per study

        self.already_uploaded_hashes = self.get_already_transferred_hashes()
        self.uploaded = []
        self.errors = []
        self.depth = 1

        self.logger = self.setup_logging()

    def setup_logging(self):
        """
        Set up logging for the FileTransfer class.

        Returns:
            logger: Configured logger instance.
        """
        logger = logging.getLogger("FileTransfer")
        logger.setLevel(logging.DEBUG if self.verbose else logging.INFO)

        # Create log directory if it doesn't exist
        log_dir = Path.home() / ".config" / TOOL_NAME / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)

        # Log file per run, with timestamp
        log_file = (
            log_dir / f'file_transfer_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        )

        # Create file handler
        fh = logging.FileHandler(log_file)
        fh.setLevel(logging.DEBUG if self.verbose else logging.INFO)

        # Create console handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG if self.verbose else logging.INFO)

        # Create formatter and add it to the handlers
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        # Add handlers to the logger
        logger.addHandler(fh)
        logger.addHandler(ch)

        logger.info("Logging initialized.")
        return logger

    def execute_query(self, query):
        """
        Execute a SQL query on the database.

        Args:
            query (str): SQL query to be executed.
        """
        with sql.connect(self.database_path) as conn:
            cursor = conn.cursor()
            cursor.execute(query)

    def execute_parameterized_query(self, query, params):
        """
        Execute a parameterized SQL query.

        Args:
            query (str): SQL query to be executed.
            params (tuple): Parameters to insert into the query.
        """
        with sql.connect(self.database_path) as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)

    def compute_file_hash(self, file_path):
        """
        Compute SHA256 hash of the given file.

        Args:
            file_path (str): Path to the file.

        Returns:
            str: SHA256 hash of the file.
        """
        sha256_hash = hashlib.sha256()
        try:
            with open(file_path, "rb") as f:
                # Read and update hash in chunks to avoid using too much memory
                for byte_block in iter(lambda: f.read(4096), b""):
                    sha256_hash.update(byte_block)
            return sha256_hash.hexdigest()

        except Exception as e:
            self.logger.error(f"Error computing hash for {file_path}: {e}")
            return None

    def get_already_transferred_hashes(self):
        """
        Fetch hashes of files that have already been transferred from the database.

        Returns:
            set: Set of file hashes.
        """
        with sql.connect(self.database_path) as conn:
            cursor = conn.cursor()
            # Ensure the 'file_hash' column exists
            cursor.execute("PRAGMA table_info(uploads);")
            columns = [col[1] for col in cursor.fetchall()]
            if "file_hash" not in columns:
                raise ValueError(
                    "The 'file_hash' column does not exist in the 'uploads' table."
                )

            cursor.execute("SELECT file_hash FROM uploads WHERE file_hash IS NOT NULL")
            result = cursor.fetchall()

        return set(item[0] for item in result)

    def add_to_database(self, study, success, src, file_name, file_hash):
        """
        Add transfer details to the database.

        Args:
            study (str): The name of the study.
            success (bool): Whether the file transfer was successful.
            src (str): The source file path.
            file_name (str): The file name.
            file_hash (str): The SHA256 hash of the file.
        """
        timestamp = datetime.now()
        if success:
            query = "INSERT INTO uploads (date, study, root_dir, file, file_hash) VALUES (?, ?, ?, ?, ?)"
            params = (timestamp, study, src, file_name, file_hash)
            self.uploaded.append(params)
            self.logger.info(f"Successfully transferred {file_name} from {src}")
        else:
            query = "INSERT INTO errors (date, study, error) VALUES (?, ?, ?)"
            params = (timestamp, study, f"Failed to transfer {file_name} from {src}")
            self.errors.append(params)
            self.logger.error(f"Failed to transfer {file_name} from {src}")

        self.execute_parameterized_query(query, params)

    def transfer_file(self, src, dest):
        """
        Transfer a file from the source to the destination directory.

        Args:
            src (str): Source file path.
            dest (str): Destination file path.

        Returns:
            bool: True if transfer was successful, False otherwise.
        """
        try:
            shutil.copy2(src, dest)  # copy2 to preserve metadata
            self.logger.info(f"Transferred {src} to {dest}")
            return True
        except Exception as e:
            self.logger.error(f"Error transferring file {src} to {dest}: {e}")
            return False

    def transfer(self, *studies):
        """
        Transfer all files from the root directories for the specified studies.

        Args:
            studies (list): List of study names to transfer files for.
                            If not specified, all studies will be processed.
        """
        for study in self.get_searchable_studies(studies):
            paths = self.root_directories[study]
            input_path = Path(paths["input_path"])
            output_path = Path(paths["output_path"])

            self.logger.info(f"Processing study: {study}")
            self.logger.info(f"Input path: {input_path}")
            self.logger.info(f"Output path: {output_path}")

            output_path.mkdir(parents=True, exist_ok=True)

            # Use tqdm for progress tracking
            for root, _, files in tqdm(
                self.walk_level(input_path, self.depth), desc=f"Processing {study}"
            ):
                for file in tqdm(
                    files, desc="Transferring files", leave=False, ncols=100
                ):
                    if file.endswith(".zip"):
                        src = Path(root) / file

                        file_hash = self.compute_file_hash(src)
                        if not file_hash:
                            self.logger.error(
                                f"Skipping {src} due to hash computation error."
                            )
                            continue

                        if file_hash in self.already_uploaded_hashes:
                            self.logger.info(
                                f"File {file} already transferred. Skipping."
                            )
                            continue

                        final_dest = output_path / file

                        success = self.transfer_file(src, final_dest)

                        if success:
                            self.add_to_database(study, True, str(src), file, file_hash)
                            self.already_uploaded_hashes.add(file_hash)
                        else:
                            self.add_to_database(
                                study, False, str(src), file, file_hash
                            )

    def insert_into_database(self, data):
        """
        Bulk insert data into the uploads table.

        Args:
            data (list): List of tuples to insert into the uploads table.
        """
        with sql.connect(self.database_path) as conn:
            cursor = conn.cursor()
            cursor.executemany(
                "INSERT INTO uploads (date, study, root_dir, file, file_hash) VALUES (?, ?, ?, ?, ?);",
                data,
            )

    def get_searchable_studies(self, studies):
        """
        Get the studies that are available for processing.

        Args:
            studies (list): List of study names to search.

        Returns:
            list: List of valid study names.
        """

        if not self.root_directories:
            return []

        if not studies:
            return list(self.root_directories.keys())

        valid_studies = [study for study in studies if study in self.root_directories]
        invalid_studies = set(studies) - set(valid_studies)
        if invalid_studies:
            self.logger.warning(f"Some studies not found: {', '.join(invalid_studies)}")

        return valid_studies

    def walk_level(self, directory, level):
        """
        Walk through a directory up to a specified depth level.

        Args:
            directory (Path): The directory path to traverse.
            level (int): The depth level for the directory traversal.

        Yields:
            tuple: Root, directories, and files at the current level.
        """
        directory = str(directory.resolve())
        num_sep = directory.count(os.path.sep)
        for root, dirs, files in os.walk(directory):
            yield root, dirs, files
            num_sep_this = root.count(os.path.sep)
            if num_sep + level <= num_sep_this:
                del dirs[:]

    def fill_database(self, *studies):
        """
        Fill the database with all files from the specified studies without transferring them.

        Args:
            studies (list): List of study names to process.
                            If not specified, all studies will be processed.
        """
        for study in self.get_searchable_studies(studies):
            paths = self.root_directories[study]
            input_path = Path(paths["input_path"])

            self.logger.info(f"Filling database for study: {study}")

            # Add tqdm for progress tracking
            for root, _, files in tqdm(
                self.walk_level(input_path, self.depth), desc=f"Processing {study}"
            ):
                for file in tqdm(files, desc="Inserting files", leave=False, ncols=100):
                    if file.endswith(".zip"):
                        src = Path(root) / file

                        file_hash = self.compute_file_hash(src)
                        if not file_hash:
                            self.logger.error(
                                f"Error computing hash for {src}. Skipping."
                            )
                            continue

                        if file_hash in self.already_uploaded_hashes:
                            self.logger.info(
                                f"File {file} already in database. Skipping."
                            )
                            continue

                        self.uploaded.append(
                            (datetime.now(), study, str(src), file, file_hash)
                        )
                        self.already_uploaded_hashes.add(file_hash)

        self.insert_into_database(self.uploaded)
        self.logger.info("Finished inserting data into the database.")

    def create_log(self):
        """
        Create a log file that contains all uploaded and error records for the current run, in JSON format.
        """
        if not self.uploaded and not self.errors:
            self.logger.info("No records to log.")
            return

        # Create a log directory
        log_dir = Path.home() / "Downloads"
        log_dir.mkdir(parents=True, exist_ok=True)

        log_file = log_dir / f'run_log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'

        log_data = {}

        # Prepare uploaded and error data for JSON output
        if self.uploaded:
            log_data["uploaded"] = [
                {
                    "study": record[1],
                    "root_dir": record[2],
                    "file": record[3],
                    "file_hash": record[4],
                }
                for record in self.uploaded
            ]

        if self.errors:
            log_data["errors"] = [
                {"study": record[1], "error": record[2]} for record in self.errors
            ]

        # Write data to JSON file
        with open(log_file, "w") as json_file:
            json.dump(log_data, json_file, indent=4)

        self.logger.info(f"Run log created at {log_file}")


class DicomProcessTransfer(FileTransfer):
    """
    A class for transferring DICOM files from one location to another.
    """

    def transfer_file(self, src, dest):
        """
        Transfer a file by processing its contents and placing the output in the correct directory.

        Args:
            src (str): Source file path (the .zip file to be processed).
            dest (str): Destination directory path (where the processed files will go).

        Returns:
            bool: True if transfer was successful, False otherwise.
        """
        # Strip the .zip extension from the destination path to create a directory name
        dest_dir = Path(dest).with_suffix("")  # Remove the '.zip' extension

        # Ensure the destination directory exists
        if not dest_dir.exists():
            dest_dir.mkdir(parents=True, exist_ok=True)

        # Initialize processor with the directory (without the '.zip') and 'dicoms'
        processor = Process(dest_dir, "dicoms")

        try:
            # Process the zip file (src)
            processor.process_zip(src)
            results = processor.results

            # Check if there are any zip errors and raise an exception if found
            if results.get("zip_errors") and any(results["zip_errors"].values()):
                raise Exception(f"Zip errors encountered: {results['zip_errors']}")

            # Log DICOM errors and timeout errors, if any
            if results.get("dicom_errors") and any(results["dicom_errors"].values()):
                self.logger.warning(f"DICOM errors: {results['dicom_errors']}")

            if results.get("timeout_errors") and any(
                results["timeout_errors"].values()
            ):
                self.logger.warning(f"Timeout errors: {results['timeout_errors']}")

            return True
        except Exception as e:
            self.logger.error(f"Error transferring file {src} to {dest_dir}: {e}")
            return False
