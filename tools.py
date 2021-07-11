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
        self.files = self.get_files()
        self.missing_days = self.get_missing()

    def get_all_dates(self):
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
        for file in files:
            splitted = file.split()
            if len(splitted) == 8:
                file_dirs.append(splitted[7])
        return file_dirs

    def get_missing(self):
        dates = set()
        for path in self.files:
            print(path)
            match = re.search(r'\d{4}-\d{2}-\d{2}', path)
            date = str(datetime.datetime.strptime(match.group(), '%Y-%m-%d').date())
            dates.add(date)
        return dates.difference(self.all_dates)

    def missing(self):
        print(self.missing_days)

    def ls(self):
        for path in self.files:
            print(path)

    def run(self):
        wdir_19 = '/var/twitter/decahose/raw'
        wdir_18 = '/var/twitter/decahose/json/'
        date = ["%.2d" % i for i in range(1, 32)]  # change this to other dates
        twitter_path = ['decahose.2018-03-{}*'.format(i) for i in date]  # chenge year and month to get more data
        variables_to_output = ['id_str', 'text', 'extended_tweet', 'user', 'created_at', 'entities']
        ret_csv_name = ['Mar/mar_{}'.format(i) for i in date]

        def n_tags(s):
            return len(s) > 0

        n_tags_udf = udf(n_tags, BooleanType())

        for i in range(len(date))[1:]:
            df = sqlContext.read.json(os.path.join(wdir_18, twitter_path[i]))
            print(date[i])
            df = df.filter(df.lang == 'en').filter(df.retweeted_status.isNull())
            df = df.withColumn('hashtags', df['entities']['hashtags'])
            df = df.filter(n_tags_udf('hashtags'))
            df.select(variables_to_output).write.mode('overwrite').parquet(ret_csv_name[i])
            df.select('id_str').write.mode('overwrite').parquet('id_only.parquet')
