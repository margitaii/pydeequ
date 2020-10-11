import unittest

from pyspark.sql import SparkSession

from pydeequ.base import VerificationSuite, AnalysisRunner, ConstraintSuggestionRunner
from pydeequ.profiler import ColumnProfilerRunner
from pydeequ.examples import test_data

class VerificationTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession
                      .builder
                      .master('local[*]')
                      .config('spark.jars.packages',
                              'com.amazon.deequ:deequ:1.0.5')
                      .appName('pytest-pyspark-local-testing')
                      .getOrCreate())
        cls.df = cls.spark.createDataFrame(test_data)

    @classmethod
    def tearDownClass(cls):
        cls.spark.sparkContext._gateway.close()
        cls.spark.stop()

    def test_VerificationSuiteArgs(self):
        suiterunner = VerificationSuite(self.spark).onData(self.df)
        # check dataframe prop
        self.assertEqual(suiterunner.dataFrame.columns,
                         ['_1', '_2', '_3', '_4', '_5']
        )
        # check _jsparkSession prop
        self.assertEqual(suiterunner._jsparkSession.getClass().toString(),
                         'class org.apache.spark.sql.SparkSession'
        )
        # check _jvm prop
        self.assertEqual(suiterunner._jvm,
                         self.spark.sparkContext._jvm
        )
        # check jvmVerificationRunBuilder
        self.assertEqual(suiterunner.jvmVerificationRunBuilder.getClass().toString(),
                         "class com.amazon.deequ.VerificationRunBuilder"
        )

    def test_AnalyzerRunnerArgs(self):
        runner = AnalysisRunner(self.spark).onData(self.df)
        # check dataframe prop
        self.assertEqual(runner.dataFrame.columns,
                         ['_1', '_2', '_3', '_4', '_5']
        )
        # check _jsparkSession prop
        self.assertEqual(runner._jsparkSession.getClass().toString(),
                         'class org.apache.spark.sql.SparkSession'
        )
        # check _jvm prop
        self.assertEqual(runner._jvm,
                         self.spark.sparkContext._jvm
        )
        # check jvmAnalysisRunBuilder
        self.assertEqual(runner.jvmAnalysisRunBuilder.getClass().toString(),
                         "class com.amazon.deequ.analyzers.runners.AnalysisRunBuilder"
        )

    def test_ProfilerRunnerArgs(self):
        profilerrunner = ColumnProfilerRunner().onData(self.df)
        # check dataframe prop
        self.assertEqual(profilerrunner.dataFrame.columns,
                         ['_1', '_2', '_3', '_4', '_5']
        )
        # check _jvm prop
        self.assertEqual(profilerrunner._jvm,
                         self.spark.sparkContext._jvm
        )
        # check jvmColumnProfilerRunBuilder
        self.assertEqual(profilerrunner.jvmColumnProfilerRunBuilder.getClass().toString(),
                         "class com.amazon.deequ.profiles.ColumnProfilerRunBuilder"
        )

    def test_SuggestionRunnerArgs(self):
        suggestionrunner = ConstraintSuggestionRunner(self.spark).onData(self.df)
        # check dataframe prop
        self.assertEqual(suggestionrunner.dataFrame.columns,
                         ['_1', '_2', '_3', '_4', '_5']
        )
        # check _jvm prop
        self.assertEqual(suggestionrunner._jvm,
                         self.spark.sparkContext._jvm
        )
        # check jvmColumnProfilerRunBuilder
        self.assertEqual(suggestionrunner.jvmConstraintSuggestionRunBuilder.getClass().toString(),
                         "class com.amazon.deequ.suggestions.ConstraintSuggestionRunBuilder"
        )

if __name__ == '__main__':
    unittest.main()

