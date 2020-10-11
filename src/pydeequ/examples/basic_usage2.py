#!/bin/bash python3

from pyspark.sql import SparkSession, DataFrame

from pydeequ.base import VerificationSuite
from pydeequ.checks import Check
from pydeequ.examples import test_data

def main():
    # SparkSession startup
    spark = (SparkSession
              .builder
              .master('local[*]')
              .config('spark.jars.packages',
                      'com.amazon.deequ:deequ:1.0.5')
              .appName('constrain-example')
              .getOrCreate())
    df = spark.createDataFrame(test_data)
    df.show()
    print(df._jdf.__doc__)

    #spark.stop()

if __name__ == '__main__':
    main()
