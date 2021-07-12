from tools import Tools

spark.conf.set("spark.sql.debug.maxToStringFields", 1000)

t = Tools(sqlContext)

t.save_processed_df('2018-03-03')
