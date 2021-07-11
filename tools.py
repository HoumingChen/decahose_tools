import os
import datetime
import subprocess
import re
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import udf


class Tools():
    '''This class provides tools for getting decahose data'''

    def __init__(self, year = 2018):
        self.year = year
        self.all_dates = self.get_all_dates()
        self.files, self.contained_dates = self.get_files()
        self.missing_days = self.all_dates.difference(self.contained_dates)

    def get_all_dates(self):
        '''Get all dates in this year'''
        start_date = datetime.date(self.year, 1, 1)
        end_date = datetime.date(self.year, 12, 31)
        delta = datetime.timedelta(days=1)
        dates = set()
        while start_date <= end_date:
            dates.add(str(start_date))
            start_date += delta
        return dates

    def get_files(self):
        cmd_var = 'hdfs dfs -ls /var/twitter/decahose/json/'
        files_var = subprocess.check_output(cmd_var, shell=True).decode().strip().split('\n')
        files = files_var
        file_dirs = []
        contained_dates = set()
        for file in files:
            splitted = file.split()
            if len(splitted) == 8:
                matched_date = re.search(r'\d{4}-\d{2}-\d{2}', splitted[7])
                if matched_date:
                    file_dirs.append(splitted[7])
                    contained_dates.add(str(datetime.datetime.strptime(matched_date.group(), '%Y-%m-%d').date()))
        return file_dirs, contained_dates

    def missing(self):
        print(self.missing_days)

    def ls(self):
        for path in self.files:
            print(sorted(list(self.missing_days)))

    def get_df(self, sql_context, date):
        if date not in self.contained_dates:
            print(f"Does not contain data on {date}")
            return None
        else:
            path = f'/var/twitter/decahose/json/decahose.{date}*'
            return sql_context.read.json(path)


