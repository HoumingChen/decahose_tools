# decahose_tools
Tools for getting the decahose data.

After cloning and cd to the repo, run this
~~~
./open_spark.sh
~~~

After entering the pyspark run following python codes
~~~
from tools import Tools
t = Tools(sqlContext)
t.run_all()
~~~