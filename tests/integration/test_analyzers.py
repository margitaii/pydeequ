import unittest

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame, Row

from pydeequ.base import AnalysisRunner
from pydeequ.examples import test_data
from pydeequ import analyzers

class AnalysisRunnerTest(unittest.TestCase):

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
        cls.runner = AnalysisRunner(cls.spark)

    @classmethod
    def tearDownClass(cls):
        cls.spark.sparkContext._gateway.close()
        cls.spark.stop()

    def test_ApproxCountDistinct(self):
        out = self.runner.onData(self.df) \
            .addAnalyzer(analyzers.ApproxCountDistinct('_1')) \
            .run().successMetricsAsDataFrame()
        out = out.select('value').collect()
        self.assertEqual(out, [Row(value=5.0)])

    def test_ApproxQuantile(self):
        out = self.runner.onData(self.df) \
            .addAnalyzer(analyzers.ApproxQuantile('_2', 0.75)) \
            .run().successMetricsAsDataFrame()
        out = out.select('value').collect()
        self.assertEqual(out, [Row(value=20)])
   
    def test_Completeness(self):
        out = self.runner.onData(self.df) \
            .addAnalyzer(analyzers.Completeness('_2')) \
            .run().successMetricsAsDataFrame()
        out = out.select('value').collect()
        self.assertEqual(out, [Row(value=0.75)])

    def test_Compliance(self):
        out = self.runner.onData(self.df) \
            .addAnalyzer(analyzers.Compliance('top _2', '_2 > 15')) \
            .run().successMetricsAsDataFrame()
        out = out.select('value').collect()
        self.assertEqual(out, [Row(value=0.25)])

    def test_Correlation(self):
        out = self.runner.onData(self.df) \
            .addAnalyzer(analyzers.Correlation('_2', '_5')) \
            .run().successMetricsAsDataFrame()
        out = out.select('value').collect()
        self.assertLess(out, [Row(value=-0.8)])

    def test_CountDistinct(self):
        out = self.runner.onData(self.df) \
            .addAnalyzer(analyzers.CountDistinct('_3')) \
            .run().successMetricsAsDataFrame()
        out = out.select('value').collect()
        self.assertEqual(out, [Row(value=3)])

    def test_DataType(self):
        out = self.runner.onData(self.df) \
            .addAnalyzer(analyzers.DataType('_3')) \
            .run().successMetricsAsDataFrame()
        out = out.select('value').collect()
        self.assertEqual(out, [Row(value=5.0), Row(value=0.0), Row(value=0.0), Row(value=0.0), Row(value=0.0), Row(value=0.0), Row(value=0.0), Row(value=0.0), Row(value=0.0), Row(value=8.0), Row(value=1.0)])

    def test_Distinctness(self):
        out = self.runner.onData(self.df) \
            .addAnalyzer(analyzers.Distinctness('_3')) \
            .run().successMetricsAsDataFrame()
        out = out.select('value').collect()
        self.assertEqual(out, [Row(value=0.375)])

    def test_Entropy(self):
        out = self.runner.onData(self.df) \
            .addAnalyzer(analyzers.Entropy('_3')) \
            .run().successMetricsAsDataFrame()
        out = out.select('value').collect()
        self.assertGreater(out, [Row(value=1)])

    def test_Histogram(self):
        out = self.runner.onData(self.df) \
            .addAnalyzer(analyzers.Histogram('_3')) \
            .run().successMetricsAsDataFrame()
        out = out.select('value').collect()
        self.assertEqual(out, [Row(value=3.0), Row(value=4.0), Row(value=0.5), Row(value=2.0), Row(value=0.25), Row(value=2.0), Row(value=0.25)])

    def test_Maximum(self):
        out = self.runner.onData(self.df) \
            .addAnalyzer(analyzers.Maximum('_2')) \
            .run().successMetricsAsDataFrame()
        out = out.select('value').collect()
        self.assertEqual(out, [Row(value=20)])

    def test_MaxLength(self):
        out = self.runner.onData(self.df) \
            .addAnalyzer(analyzers.MaxLength('_1')) \
            .run().successMetricsAsDataFrame()
        out = out.select('value').collect()
        self.assertEqual(out, [Row(value=6)])
    
    def test_Mean(self):
        out = self.runner.onData(self.df) \
            .addAnalyzer(analyzers.Mean('_2')) \
            .run().successMetricsAsDataFrame()
        out = out.select('value').collect()
        self.assertEqual(out, [Row(value=11)])

    def test_Minimum(self):
        out = self.runner.onData(self.df) \
            .addAnalyzer(analyzers.Minimum('_2')) \
            .run().successMetricsAsDataFrame()
        out = out.select('value').collect()
        self.assertEqual(out, [Row(value=1)])

    def test_MinLength(self):
        out = self.runner.onData(self.df) \
            .addAnalyzer(analyzers.MaxLength('_1')) \
            .run().successMetricsAsDataFrame()
        out = out.select('value').collect()
        self.assertEqual(out, [Row(value=6)])

    def test_MutualInformation(self):
        out = self.runner.onData(self.df) \
            .addAnalyzer(analyzers.MutualInformation(['_1', '_3'])) \
            .run().successMetricsAsDataFrame()
        out = out.select('value').collect()
        self.assertGreater(out, [Row(value=0.5)])

    def test_Size(self):
        out = self.runner.onData(self.df) \
            .addAnalyzer(analyzers.Size()) \
            .run().successMetricsAsDataFrame()
        out = out.select('value').collect()
        self.assertEqual(out, [Row(value=8)])
    
    def test_StandardDeviation(self):
        out = self.runner.onData(self.df) \
            .addAnalyzer(analyzers.StandardDeviation('_2')) \
            .run().successMetricsAsDataFrame()
        out = out.select('value').collect()
        self.assertGreater(out, [Row(value=7)])

    def test_Sum(self):
        out = self.runner.onData(self.df) \
            .addAnalyzer(analyzers.Sum('_2')) \
            .run().successMetricsAsDataFrame()
        out = out.select('value').collect()
        self.assertGreater(out, [Row(value=10)])

    def test_Uniqueness(self):
        out = self.runner.onData(self.df) \
            .addAnalyzer(analyzers.Uniqueness(['_1'])) \
            .run().successMetricsAsDataFrame()
        out = out.select('value').collect()
        self.assertEqual(out, [Row(value=0.375)])    

    def test_UniqueValueRatio(self):
        out = self.runner.onData(self.df) \
            .addAnalyzer(analyzers.UniqueValueRatio(['_1'])) \
            .run().successMetricsAsDataFrame()
        out = out.select('value').collect()
        self.assertEqual(out, [Row(value=0.6)])   

if __name__ == '__main__':
    unittest.main()
        