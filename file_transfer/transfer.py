import json
import os
import zipfile
from collections import defaultdict

import pydicom
from tqdm import tqdm


class Process:
    def __init__(self, directory, output_path="clean"):
        self.directory = directory
        self.output_path = os.path.join(self.directory, output_path)

        if not os.path.exists(self.output_path):
            os.makedirs(self.output_path)

        # Initialize error tracking
        self.results = {"success": 0, "zip_errors": defaultdict(int), "dicom_errors": defaultdict(int)}

    def process_dir(self):
        # Get the list of all ZIP files in the directory
        zip_files = [f for f in os.listdir(self.directory) if f.endswith(".zip")]

        # Use tqdm to show a progress bar for the ZIP files processing
        for filename in tqdm(zip_files, desc="Processing ZIP files", unit="file"):
            full_path = os.path.join(self.directory, filename)
            self.process_zip(full_path)

        # Write error/success results to a JSON file
        json_output_path = os.path.join(self.output_path, "processing_results.json")
        with open(json_output_path, "w") as json_file:
            json.dump(self.results, json_file, indent=4)

    def process_zip(self, zip_path):
        name_without_extension = os.path.splitext(os.path.basename(zip_path))[0]
        ct = 0
        try:
            with zipfile.ZipFile(zip_path, "r") as zip_file:
                for local_file_name in zip_file.namelist():
                    try:
                        with zip_file.open(local_file_name) as f:
                            ds = pydicom.dcmread(f)

                        # Process the DICOM file
                        self.process_dicom(ds, name_without_extension, ct)
                        ct += 1
                        self.results["success"] += 1  # Track success

                    except Exception as dicom_error:
                        # Track DICOM processing errors
                        self.results["dicom_errors"][name_without_extension] += 1

        except Exception as zip_error:
            # Track ZIP file processing errors
            self.results["zip_errors"][name_without_extension] += 1

    def process_dicom(self, ds, name_without_extension, ct, decompress=False):
        if ds.is_little_endian and decompress:
            ds.decompress()

        ds.PatientName = name_without_extension
        l_name = f"{ct}.dcm"
        local_path = os.path.join(self.output_path, name_without_extension)

        if not os.path.exists(local_path):
            os.makedirs(local_path)

        ds.save_as(os.path.join(local_path, l_name))