# decahose_tools
These are the codes I used on the cavium-thunderx to get, select, and preprocess data.

# Getting started
1. Connect to the cavium-thunderx server, start a screen, clone the repo, and cd to the repo.
~~~
screen
git clone https://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@github.com/HoumingChen/decahose_tools.git
cd decahose_tools
~~~

2. After cloning and cd to the repo, run this to start the pyspark.
~~~
./open_spark.sh
~~~

3. After entering the pyspark run following python codes
~~~
from tools import Tools
t = Tools(sqlContext)
t.run_all()
~~~
