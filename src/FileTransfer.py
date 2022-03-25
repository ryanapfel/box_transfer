import pandas as pd
from datetime import datetime
import shutil
import os
import sqlite3 as sql
import xlsxwriter
import re


class FileTransfer:
    def __init__(self, database_path, rootDirectories, destDir, verbose=False):
        self.verbose = verbose
        self.database_path = database_path
        self.rootDirectories = rootDirectories
        self.destDir = destDir
        self.alreadyUploaded = self.already_transfered()
        self.uploaded = []
        self.errors = []
        self.DEPTH = 1

    def __repr__(self):
        printString = f'''
        {self.database_path} = database_path
        {self.rootDirectories} = rootDirectories
        {self.destDir} = destDir
        '''
        return printString

    def run_query(self, q):
        with sql.connect(self.database_path) as conn:
            cursor = conn.cursor()
            cursor.execute(q)

    def paramterized_query(self, q, params):
        with sql.connect(self.database_path) as conn:
            cursor = conn.cursor()
            cursor.execute(q, params)

    '''
    args: src dest of files
    returns: True if transfer succesful
    '''

    def moveFiles(self, src):
        if self.verbose:
            print(src, self.destDir)

        try:
            shutil.copy(src, self.destDir)
            return True
        except Exception as e:
            print(e)
            return False

    def addToDB(self, study, success, src, file):
        if success:
            query = f'''INSERT INTO uploads VALUES (?,?,?,?)'''
            params = (datetime.now(), study, src, file)
            self.uploaded.append(params)
        else:
            query = f'''INSERT INTO errors VALUES (?,?,?)'''
            params = (datetime.now(), study, src)
            self.errors.append(params)

        if self.verbose and success:
            print(f'{study} -- moved {file} to destination')
        elif self.verbose and not success:
            print(f'{study} -- ERROR moving {file} to destination')

        self.paramterized_query(query, params)

    def already_transfered(self):
        with sql.connect(self.database_path) as conn:
            cursor = conn.cursor()
            cursor.execute(f'''SELECT file FROM uploads''')
            sqlOut = cursor.fetchall()

        return [item for sublist in sqlOut for item in sublist]

    def transfer(self, *args):
        studiesMoved = 0

        for study in self.getSearchableStudies(args):
            path = self.rootDirectories[study]
            for root, _, files in self.walklevel(path, self.DEPTH):
                for file in files:
                    src = f'{root}/{file}'
                    if file.endswith('.zip') and file not in self.alreadyUploaded:
                        succesful = self.moveFiles(src)
                        self.addToDB(study, succesful, src, file)
                        studiesMoved += 1
                    elif file.endswith('.pdf') and file not in self.alreadyUploaded:
                        # TODO: store pdf's somewhere as they might be valueable
                        pass
                    elif file in self.alreadyUploaded and self.verbose:
                        print(f'Already added {file}')

    def insert_into(self, to_insert):
        with sql.connect(self.database_path) as conn:
            cursor = conn.cursor()
            cursor.executemany(
                'INSERT INTO uploads VALUES(?,?,?,?);', to_insert)

    def reset_db(self, *args):
        if not args:
            query = f'''DELETE FROM uploads'''
            self.run_query(query)
        else:
            for arg in args:
                query = f"""DELETE FROM uploads WHERE study LIKE '%{arg}%'"""
                self.run_query(query)

    def getSearchableStudies(self, args):
        searchStudies = []

        if not args:
            searchStudies = self.rootDirectories.keys()
        else:
            for arg in args:
                if arg in self.rootDirectories:
                    searchStudies.append(arg)
                else:
                    print(f'{arg} not a valid study, please check declaration')

        print('Searching : ', ' '.join(searchStudies))

        return searchStudies

    def walklevel(self, dir, level):
        some_dir = dir.rstrip(os.path.sep)
        assert os.path.isdir(some_dir)
        num_sep = some_dir.count(os.path.sep)
        for root, dirs, files in os.walk(some_dir):
            yield root, dirs, files
            num_sep_this = root.count(os.path.sep)
            if num_sep + level <= num_sep_this:
                del dirs[:]

    def fillDataBase(self, *args):

        for study in self.getSearchableStudies(args):
            path = self.rootDirectories[study]
            for root, _, files in self.walklevel(path, self.DEPTH):
                for file in files:
                    if file.endswith('.zip') and file not in self.alreadyUploaded:
                        src = f'{root}/{file}'
                        self.uploaded.append((True, study, src, file))

        self.insert_into(self.uploaded)
        print("Finished Inserting")

    def create_log(self, output_path):
        df = pd.DataFrame(self.uploaded, columns=[
                          'Date', 'Study', 'Path', 'File'])

        errors = pd.DataFrame(self.errors, columns=['Date', 'Study', 'File'])

        writer = pd.ExcelWriter(output_path, engine='xlsxwriter')
        df.to_excel(writer, sheet_name='Uploaded')
        errors.to_excel(writer, sheet_name='Errors')

        worksheet = writer.sheets['Uploaded']
        errorWorksheet = writer.sheets['Errors']
        # Get the dimensions of the dataframe.
        (max_row, max_col) = df.shape
        # Set the column widths, to make the dates clearer.
        worksheet.set_column(0, max_col, 20)

        (max_row, max_col) = errors.shape
        errorWorksheet.set_column(0, max_col, 20)

        writer.save()

    def master_log(self, output_path):
        conn = sql.connect(self.database_path)

        df = pd.read_sql("SELECT * FROM uploads", conn)

        writer = pd.ExcelWriter(output_path, engine='xlsxwriter')
        df.to_excel(writer, sheet_name='Uploaded')
        worksheet = writer.sheets['Uploaded']
        (max_row, max_col) = df.shape
        worksheet.set_column(0, max_col, 20)

        # close connections
        writer.save()
        conn.close()
