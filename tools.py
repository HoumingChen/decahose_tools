import os
import datetime
import subprocess
import re
import time
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import udf

#spark.conf.set("spark.sql.debug.maxToStringFields", 1000)

class Tools():
    '''This class provides tools for getting decahose data'''

    def __init__(self, sqlContext, year=2018):
        self.sqlContext = sqlContext
        self.year = year
        self.all_dates = self.__get_all_dates()
        self.files, self.contained_dates = self.__get_files()
        self.missing_days = self.all_dates.difference(self.contained_dates)
        self.tag_topic_dict = self.__load_predefined_tags()
        self.all_tags = frozenset(self.tag_topic_dict.keys())
        self.date_list = sorted(list(self.all_dates.difference(self.missing_days)))

    def __get_all_dates(self):
        '''Get all dates in this year'''
        start_date = datetime.date(self.year, 1, 1)
        end_date = datetime.date(self.year, 12, 31)
        delta = datetime.timedelta(days=1)
        dates = set()
        while start_date <= end_date:
            dates.add(str(start_date))
            start_date += delta
        return dates

    def __get_files(self):
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

    def __load_predefined_tags(self):
        f = open('no_label_tag_topic.txt', 'r')
        input_file = f.readlines()
        tag_name = [input_file[i].strip() for i in range(2, len(input_file), 2)]
        tag_topic = [input_file[i].strip() for i in range(3, len(input_file), 2)]
        tag_topic_dict = {tag_name[i]: tag_topic[i] for i in range(len(tag_name))}

        return tag_topic_dict

    def get_filter(self):
        '''Copying the 'self.all_tags' to 'tags' is necessary. Don't write in one line.'''
        tags = self.all_tags
        return udf(lambda entities: any(element.text.lower() in tags for element in entities.hashtags), BooleanType())


    def missing(self):
        print(sorted(list(self.missing_days)))

    def ls(self):
        for path in self.files:
            print(path)

    def get_raw_df(self, date):
        if date not in self.contained_dates:
            print(f"Does not contain data on {date}")
            return None
        else:
            path = f'/var/twitter/decahose/json/decahose.{date}*'
            return self.sqlContext.read.json(path)

    def get_df(self, date):
        df = self.get_raw_df(date)
        my_filter = self.get_filter()
        return df.filter((df.lang == 'en') & (df.retweeted_status.isNull())).filter(my_filter('entities'))

    def save_processed_df(self, date, silent = True):
        if not silent:
            print(f"{date}: geting data")
        df = self.get_df(date)
        time.sleep(1)
        if not silent:
            print(f"{date}: saving data")
        df.select('id_str').write.mode('overwrite').parquet(os.path.join("decahose_500tag_data", date + '_id.parquet'))
        print(f"{date}: finished!")

    def run_all(self, start_index = 0, end_index = None):
        if end_index is None:
            end_index = len(self.date_list)
        for i in range(start_index, end_index):
            try:
                self.save_processed_df(self.date_list[i], silent=False)
                time.sleep(5)
            except Exception as e:
                print(f"Exception occured when reading {self.date_list[i]}")
                print(e)

    def run_index(self, index):
        self.save_processed_df(self.date_list[index], silent=False)
