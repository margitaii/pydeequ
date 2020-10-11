#!/bin/bash python3

from pydeequ.examples import test_data
from pydeequ import AnalysisRunner, VerificationSuite 
import pydeequ.analyzers as analyzers
from pydeequ.metricsrepo import ResultKey, FileSystemMetricsRepository
from pydeequ.checks import Check

def main():
     # SparkSession startup
     spark = (SparkSession
               .builder
               .master('local[*]')
               .config('spark.jars.packages',
                       'com.amazon.deequ:deequ:1.0.5')
               .appName('suggestions-example')
               .getOrCreate())
     df = spark.createDataFrame(test_data)
     # Analysis run
     a = (AnalysisRunner(spark)
          .onData(df)
          .addAnalyzer(analyzers.Size())) \
          .run()
     key = ResultKey(spark, 100000, {'key1': 'value1'})
     myrepo = FileSystemMetricsRepository(spark, '../test.json')
     myrepo.save(key, a)

     # Verification run
     key2 = repo.ResultKey(spark, 100000, {'key1': 'value2', 'key2':'value3'})
     

     v = (base.VerificationSuite(spark)
              .onData(df)
              .addCheck(Check(spark, 'error', 'examples')
                        .hasSize(lambda x: x == 8)
                        .isUnique('_2'))
          .useRepository(myrepo)
          .saveOrAppendResult(key2)
          .run()
     )

     myrepo.load().withTagValues({'key1': 'value1'}).after(99000) \
          .getMetricsAsDF().show()

     # SparkSession and Java Gateway teardown
     spark.sparkContext._gateway.close()
     spark.stop()

if __name__ == "__main__":
    main()