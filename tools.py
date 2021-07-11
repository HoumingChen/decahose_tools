import os
import subprocess
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import udf


class Tools():
    '''This class provides tools for getting decahose data'''

    def __init__(self):
        pass

    def ls(self):
        cmd_var = 'hdfs dfs -ls /var/twitter/decahose/json/'
        files_var = subprocess.check_output(cmd_var, shell=True).strip().split('\n')
        cmd_data = 'hdfs dfs -ls /data/twitter/decahose/'
        files_data = subprocess.check_output(cmd_data, shell=True).strip().split('\n')
        files = files_var + files_data
        for path in files:
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
