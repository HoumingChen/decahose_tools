import os
import datetime
import subprocess
import re
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
        f = open('pre_define_tag_topic.txt', 'r')
        input_file = f.readlines()
        tag_name = [input_file[i].strip() for i in range(2, len(input_file), 2)]
        tag_topic = [input_file[i].strip() for i in range(3, len(input_file), 2)]
        tag_topic_dict = {tag_name[i]: tag_topic[i] for i in range(len(tag_name))}

        for i in tag_topic_dict.keys():
            if tag_topic_dict[i] == 'NONE':
                tag_topic_dict[i] = 'OTHER'
            elif 'MUSIC' in tag_topic_dict[i]:
                tag_topic_dict[i] = 'MUSIC'
            elif 'MOVIES' in tag_topic_dict[i]:
                tag_topic_dict[i] = 'MOVIES'
            elif 'GAMES' in tag_topic_dict[i]:
                tag_topic_dict[i] = 'GAMES'
            elif 'TECHNOLOGY' in tag_topic_dict[i]:
                tag_topic_dict[i] = 'TECHNOLOGY'

        tag_topic_dict = {key: val for key, val in tag_topic_dict.items() if val != 'CELEBRITY'}
        tag_topic_dict['Facebook'] = 'TECHNOLOGY'
        return tag_topic_dict

    def get_filter(self):
        '''Copying the 'self.all_tags' to 'tags' is necessary. Don't write in one line.'''
        tags = self.all_tags
        return udf(lambda entities: any(element.text in tags for element in entities.hashtags), BooleanType())


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

    def save_processed_df(self, date):
        df = self.get_df(date)
        df.select('id_str').write.mode('overwrite').parquet(os.path.join("decahose_500tag_data", date + '_id.parquet'))

    def run_all(self, data):
        for date in self.contained_dates:
            self.save_processed_df(date)

