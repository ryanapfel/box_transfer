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
    home_dir = Path.home()
    config_dir = home_dir / ".config" / TOOL_NAME / ""

    if not config_dir.exists():
        config_dir.mkdir(parents=True)

    db_path = config_dir / "database.sql"

    return db_path


def init_db():
    db_path = get_db_path()

    if not Path(db_path).exists():
        print(f"Creating new database at {db_path}")
        with sql.connect(db_path) as conn:
            cursor = conn.cursor()

            # Create the tables if they do not exist
            cursor.execute(
                """CREATE TABLE IF NOT EXISTS uploads
                    (date date, study text, root_dir text, file text, file_hash text, error BOOLEAN)"""
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
    with sql.connect(database_path) as conn:
        cursor = conn.cursor()

        for study in studies:
            cursor.execute("DELETE FROM uploads WHERE study = ?", (study,))
            print(f"Records for study {study} have been deleted.")

        conn.commit()


def get_studies():
    db_path = init_db()

    try:
        with sql.connect(db_path) as conn:
            cursor = conn.cursor()

            cursor.execute("SELECT study_name, input_path, output_path FROM studies")
            studies = cursor.fetchall()

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
    def __init__(self, database_path, root_directories, verbose=True):
        self.verbose = verbose
        self.database_path = database_path
        self.root_directories = root_directories
        self.already_uploaded_hashes = self.get_already_transferred_hashes()
        self.uploaded = []
        self.errors = []
        self.depth = 1
        self.logger = self.setup_logging()

    def setup_logging(self):
        logger = logging.getLogger("FileTransfer")
        logger.setLevel(logging.DEBUG if self.verbose else logging.INFO)

        log_dir = Path.home() / ".config" / TOOL_NAME / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)

        log_file = (
            log_dir / f'file_transfer_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        )

        fh = logging.FileHandler(log_file)
        fh.setLevel(logging.DEBUG if self.verbose else logging.INFO)

        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG if self.verbose else logging.INFO)

        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        logger.addHandler(fh)
        logger.addHandler(ch)

        logger.info("Logging initialized.")
        return logger

    def delete_destination(self, dest_path):
        """
        Delete all contents of the destination folder.

        Args:
            dest_path (str): Path to the destination directory.
        """
        dest_dir = Path(dest_path)
        if not dest_dir.exists():
            self.logger.warning(f"Destination directory {dest_path} does not exist.")
            return

        # Loop through the contents of the destination directory and delete them
        for item in dest_dir.iterdir():
            try:
                if item.is_file():
                    item.unlink()  # Delete file
                elif item.is_dir():
                    shutil.rmtree(item)  # Delete directory and its contents
                self.logger.info(f"Deleted {item}")
            except Exception as e:
                self.logger.error(f"Error deleting {item}: {e}")

    def execute_query(self, query):
        with sql.connect(self.database_path) as conn:
            cursor = conn.cursor()
            cursor.execute(query)

    def execute_parameterized_query(self, query, params):
        with sql.connect(self.database_path) as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)

    def compute_file_hash(self, file_path):
        """
        Compute SHA256 hash based on the file name (instead of its contents).

        Args:
            file_path (str): Path to the file or URL if it's a cloud file.

        Returns:
            str: SHA256 hash of the file name.
        """
        try:
            # Use the file name as the source for hashing
            file_name = os.path.basename(file_path)
            sha256_hash = hashlib.sha256(file_name.encode()).hexdigest()
            self.logger.info(
                f"Computed hash for {file_path} based on file name: {sha256_hash}"
            )
            return sha256_hash

        except Exception as e:
            self.logger.error(f"Error computing hash for {file_path}: {e}")
            return None

    def get_already_transferred_hashes(self):
        with sql.connect(self.database_path) as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(uploads);")
            columns = [col[1] for col in cursor.fetchall()]
            if "file_hash" not in columns:
                raise ValueError(
                    "The 'file_hash' column does not exist in the 'uploads' table."
                )

            cursor.execute("SELECT file_hash FROM uploads WHERE file_hash IS NOT NULL")
            result = cursor.fetchall()

        return set(item[0] for item in result)

    def add_to_database(self, study, success, src, file_name, file_hash, error=False):
        timestamp = datetime.now()
        query = "INSERT INTO uploads (date, study, root_dir, file, file_hash, error) VALUES (?, ?, ?, ?, ?, ?)"
        params = (timestamp, study, src, file_name, file_hash, error)
        self.uploaded.append(params)
        if success:
            self.logger.info(f"Successfully transferred {file_name} from {src}")
        else:
            self.logger.error(f"Failed to transfer {file_name} from {src}")
        self.execute_parameterized_query(query, params)

    def transfer_file(self, src, dest):
        try:
            shutil.copy2(src, dest)
            self.logger.info(f"Transferred {src} to {dest}")
            return True
        except Exception as e:
            self.logger.error(f"Error transferring file {src} to {dest}: {e}")
            return False

    def transfer_subdirectory(
        self, study, sub_directory, retry_errors=False, dry_run=False, force=False
    ):
        """
        Transfer files from a specific subdirectory of the study's input path.

        Args:
            study (str): The study name.
            sub_directory (str): The subdirectory inside the input path to process.
            retry_errors (bool): Retry files with errors.
            dry_run (bool): Simulate the transfer without transferring files.
        """
        paths = self.root_directories.get(study)
        if not paths:
            self.logger.error(f"Study {study} not found.")
            return

        input_subdir = Path(paths["input_path"]) / sub_directory
        output_path = Path(paths["output_path"])

        self.logger.info(f"Processing subdirectory {sub_directory} for study: {study}")
        self.logger.info(f"Input subdirectory: {input_subdir}")
        self.logger.info(f"Output path: {output_path}")

        # Ensure the output path exists
        if not dry_run:
            output_path.mkdir(parents=True, exist_ok=True)

        # Process the files in the subdirectory
        self.process_files_in_directory(
            study, input_subdir, output_path, retry_errors, dry_run, force
        )

    def process_files_in_directory(
        self, study, input_dir, output_dir, retry_errors, dry_run, force=False
    ):
        """
        Helper function to process all files in a given directory.

        Args:
            study (str): The study name.
            input_dir (Path): The input directory to process.
            output_dir (Path): The output directory.
            retry_errors (bool): Retry files with errors.
            dry_run (bool): Simulate the transfer without transferring files.
        """
        # Use tqdm for progress tracking
        for root, _, files in tqdm(
            self.walk_level(input_dir, self.depth), desc=f"Processing {input_dir}"
        ):
            for file in tqdm(files, desc="Transferring files", leave=False, ncols=100):
                if file.endswith(".zip"):
                    src = Path(root) / file

                    file_hash = self.compute_file_hash(src)

                    if not file_hash and not force:
                        self.logger.error(
                            f"Skipping {src} due to hash computation error."
                        )
                        continue

                    if (
                        file_hash in self.already_uploaded_hashes
                        and not retry_errors
                        and not force
                    ):
                        self.logger.info(f"File {file} already transferred. Skipping.")
                        continue

                    if dry_run:
                        self.logger.info(f"DRY RUN: Simulating transfer for {src}")
                        self.add_to_database(
                            study, True, str(src), file, file_hash, error=False
                        )
                    else:
                        final_dest = output_dir / file
                        success = self.transfer_file(src, final_dest)
                        self.add_to_database(
                            study, success, str(src), file, file_hash, not success
                        )
                        if success:
                            self.already_uploaded_hashes.add(file_hash)

    def transfer(self, *studies, retry_errors=False, dry_run=False, force=False):
        """
        Transfer all files from the root directories for the specified studies.
        Args:
            studies (list): List of study names to transfer files for.
                            If not specified, all studies will be processed.
            retry_errors (bool): Retry files with errors.
            dry_run (bool): Simulate the transfer process without actually transferring files.
        """
        for study in self.get_searchable_studies(studies):
            paths = self.root_directories[study]
            input_path = Path(paths["input_path"])
            output_path = Path(paths["output_path"])

            self.logger.info(f"Processing study: {study}")
            self.logger.info(f"Input path: {input_path}")
            self.logger.info(f"Output path: {output_path}")

            if not dry_run:
                output_path.mkdir(parents=True, exist_ok=True)

            self.process_files_in_directory(
                study, input_path, output_path, retry_errors, dry_run, force
            )

    def insert_into_database(self, data):
        with sql.connect(self.database_path) as conn:
            cursor = conn.cursor()
            cursor.executemany(
                "INSERT INTO uploads (date, study, root_dir, file, file_hash, error) VALUES (?, ?, ?, ?, ?, ?);",
                data,
            )

    def get_searchable_studies(self, studies):
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
        directory = str(directory.resolve())
        num_sep = directory.count(os.path.sep)
        for root, dirs, files in os.walk(directory):
            yield root, dirs, files
            num_sep_this = root.count(os.path.sep)
            if num_sep + level <= num_sep_this:
                del dirs[:]

    def fill_database(self, *studies):
        for study in self.get_searchable_studies(studies):
            paths = self.root_directories[study]
            input_path = Path(paths["input_path"])

            self.logger.info(f"Filling database for study: {study}")

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
                            (datetime.now(), study, str(src), file, file_hash, False)
                        )
                        self.already_uploaded_hashes.add(file_hash)

        self.insert_into_database(self.uploaded)
        self.logger.info("Finished inserting data into the database.")

    def create_log(self):
        if not self.uploaded:
            self.logger.info("No records to log.")
            return

        log_dir = Path.home() / "Downloads"
        log_dir.mkdir(parents=True, exist_ok=True)

        log_file = log_dir / f'run_log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'

        log_data = {}

        if self.uploaded:
            log_data["uploaded"] = [
                {
                    "study": record[1],
                    "root_dir": record[2],
                    "file": record[3],
                    "file_hash": record[4],
                    "error": record[5],
                }
                for record in self.uploaded
            ]

        with open(log_file, "w") as json_file:
            json.dump(log_data, json_file, indent=4)

        self.logger.info(f"Run log created at {log_file}")

    def master_log(self, output_path):
        with sql.connect(self.database_path) as conn:
            df = pd.read_sql("SELECT * FROM uploads", conn)

        log_data = df.to_dict(orient="records")

        with open(output_path, "w") as json_file:
            json.dump(log_data, json_file, indent=4)

        self.logger.info(f"Master log created at {output_path}")


class DicomProcessTransfer(FileTransfer):
    def transfer_file(self, src, dest):
        dest_dir = Path(dest).with_suffix("")

        if not dest_dir.exists():
            dest_dir.mkdir(parents=True, exist_ok=True)

        processor = Process(dest_dir, "dicoms")

        try:
            processor.process_zip(src)
            results = processor.results

            if results.get("zip_errors") and any(results["zip_errors"].values()):
                raise Exception(f"Zip errors encountered: {results['zip_errors']}")

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
