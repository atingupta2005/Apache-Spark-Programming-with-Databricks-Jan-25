"""
This is the "front door" of your application which gets submitted
to Spark and serves as the starting point.
"""
import pyspark

if __name__ == '__main__':
    spark = (
        pyspark.sql.SparkSession.builder
        .getOrCreate())

    names = (
        spark.createDataFrame(
            data=[
                ('Nick',),
                ('John',),
                ('Frank',),
            ],
            schema=['name']
        ))

    names.show()

## How to run:
# Open Anaconda Prompt
# D:
# cd D:\22-Trngs\2-Confirmed\5-PySpark-56-hours-Geet
# conda activate ./conda-env
# cd D:\22-Trngs\2-Confirmed\5-PySpark-56-hours-Geet\GH\Labs\Day6\spark-submit
# SET PYSPARK_PYTHON=python
# spark-submit  hello.py
