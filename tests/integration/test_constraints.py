import unittest

from pyspark.sql import SparkSession, DataFrame, Row

from pydeequ.base import VerificationSuite
from pydeequ.checks import Check
from pydeequ.examples import test_data

class ConstraintTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession
                      .builder
                      .master('local[*]')
                      .config('spark.jars.packages',
                              'com.amazon.deequ:deequ:1.0.3-rc2')
                      .appName('pytest-pyspark-local-testing')
                      .getOrCreate())
        cls.df = cls.spark.createDataFrame(test_data)
        cls.suite = VerificationSuite(cls.spark)
        cls.success = Row(constraint_status = 'Success')
        cls.failure = Row(constraint_status = 'Failure')

    @classmethod
    def tearDownClass(cls):
        cls.spark.sparkContext._gateway.close()
        cls.spark.stop()

    def test_hasSize(self):
        chk = Check(self.spark) \
            .hasSize(lambda x: x == 8)
        out = self.suite.onData(self.df).addCheck(chk).run()
        out = DataFrame(out, self.spark).select('constraint_status').collect()
        self.assertEqual(out, [self.success])

    def test_isUnique(self):
        chk = Check(self.spark) \
            .isUnique('_1')
        out = self.suite.onData(self.df).addCheck(chk).run()
        out = DataFrame(out, self.spark).select('constraint_status').collect()
        self.assertEqual(out, [self.failure])

    def test_hasCompleteness(self):
        chk = Check(self.spark) \
                   .hasCompleteness('_2', lambda x: x >= 0.75)
        out = self.suite.onData(self.df).addCheck(chk).run()
        out = DataFrame(out, self.spark).select('constraint_status').collect()
        self.assertEqual(out, [self.success])

    def test_hasUniqueness(self):
        chk = Check(self.spark) \
                   .hasUniqueness('_1', lambda x: x == 3/8)
        out = self.suite.onData(self.df).addCheck(chk).run()
        out = DataFrame(out, self.spark).select('constraint_status').collect()
        self.assertEqual(out, [self.success])

    def test_hasDistinctness(self):
        chk = Check(self.spark) \
                   .hasDistinctness('_1', lambda x: x == 5/8)
        out = self.suite.onData(self.df).addCheck(chk).run()
        out = DataFrame(out, self.spark).select('constraint_status').collect()
        self.assertEqual(out, [self.success])

    def test_hasUniqueValueRatio(self):
        chk = Check(self.spark) \
                   .hasUniqueValueRatio('_2', lambda x: x == 0.8)
        out = self.suite.onData(self.df).addCheck(chk).run()
        out = DataFrame(out, self.spark).select('constraint_status').collect()
        self.assertEqual(out, [self.success])

    def test_hasNumberOfDistinctValues(self):
        chk = Check(self.spark) \
                   .hasNumberOfDistinctValues('_2', lambda x: x == 6)
        out = self.suite.onData(self.df).addCheck(chk).run()
        out = DataFrame(out, self.spark).select('constraint_status').collect()
        self.assertEqual(out, [self.success])

    # .hasHistogram

    def test_hasEntropy(self):
        chk = Check(self.spark) \
                   .hasEntropy('_3', lambda x: x > 1)
        out = self.suite.onData(self.df).addCheck(chk).run()
        out = DataFrame(out, self.spark).select('constraint_status').collect()
        self.assertEqual(out, [self.success])

    # .hasMutualInformation

    def test_hasApproxQuantile(self):
        chk = Check(self.spark) \
                   .hasApproxQuantile('_2', 0.5, lambda x: x == 7)
        out = self.suite.onData(self.df).addCheck(chk).run()
        out = DataFrame(out, self.spark).select('constraint_status').collect()
        self.assertEqual(out, [self.success])

    def test_hasMinLength(self):
        chk = Check(self.spark) \
                   .hasMinLength('_1', lambda x: x == 6)
        out = self.suite.onData(self.df).addCheck(chk).run()
        out = DataFrame(out, self.spark).select('constraint_status').collect()
        self.assertEqual(out, [self.success])

    def test_hasMaxLength(self):
        chk = Check(self.spark) \
                   .hasMaxLength('_3', lambda x: x == 10)
        out = self.suite.onData(self.df).addCheck(chk).run()
        out = DataFrame(out, self.spark).select('constraint_status').collect()
        self.assertEqual(out, [self.success])

    def test_hasMin(self):
        chk = Check(self.spark) \
                   .hasMin('_2', lambda x: x == 1)
        out = self.suite.onData(self.df).addCheck(chk).run()
        out = DataFrame(out, self.spark).select('constraint_status').collect()
        self.assertEqual(out, [self.success])

    def test_hasMax(self):
        chk = Check(self.spark) \
                   .hasMax('_2', lambda x: x == 20)
        out = self.suite.onData(self.df).addCheck(chk).run()
        out = DataFrame(out, self.spark).select('constraint_status').collect()
        self.assertEqual(out, [self.success])

    def test_hasMean(self):
        chk = Check(self.spark) \
                   .hasMean('_2', lambda x: x > 10)
        out = self.suite.onData(self.df).addCheck(chk).run()
        out = DataFrame(out, self.spark).select('constraint_status').collect()
        self.assertEqual(out, [self.success])

    def test_hasSum(self):
        chk = Check(self.spark) \
                   .hasSum('_2', lambda x: x > 50)
        out = self.suite.onData(self.df).addCheck(chk).run()
        out = DataFrame(out, self.spark).select('constraint_status').collect()
        self.assertEqual(out, [self.success])

    def test_hasStandardDeviation(self):
        chk = Check(self.spark) \
                   .hasStandardDeviation('_2', lambda x: x > 5)
        out = self.suite.onData(self.df).addCheck(chk).run()
        out = DataFrame(out, self.spark).select('constraint_status').collect()
        self.assertEqual(out, [self.success])

    def test_hasApproxContDistintc(self):
        chk = Check(self.spark) \
                   .hasApproxCountDistinct('_2', lambda x: x == 5)
        out = self.suite.onData(self.df).addCheck(chk).run()
        out = DataFrame(out, self.spark).select('constraint_status').collect()
        self.assertEqual(out, [self.success])

    def test_hasCorrelation(self):
        chk = Check(self.spark) \
                   .hasCorrelation('_2', '_2', lambda x: x == 1)
        out = self.suite.onData(self.df).addCheck(chk).run()
        out = DataFrame(out, self.spark).select('constraint_status').collect()
        self.assertEqual(out, [self.success])

    def test_satisfies(self):
        chk = Check(self.spark) \
                   .satisfies("_2 > 15", "MyCondition", lambda x: x == 0.25)
        out = self.suite.onData(self.df).addCheck(chk).run()
        out = DataFrame(out, self.spark).select('constraint_status').collect()
        self.assertEqual(out, [self.success])


                   #.hasPattern("_1", "thing([A-Z])", lambda x: x == 1)
                   #.hasDataType("_1", "string", lambda x: x == 1)

    def test_isPositive(self):
        chk = Check(self.spark) \
                   .isPositive('_2')
        out = self.suite.onData(self.df).addCheck(chk).run()
        out = DataFrame(out, self.spark).select('constraint_status').collect()
        self.assertEqual(out, [self.success])

    def test_isNonNegative(self):
        chk = Check(self.spark) \
                   .isNonNegative('_2')
        out = self.suite.onData(self.df).addCheck(chk).run()
        out = DataFrame(out, self.spark).select('constraint_status').collect()
        self.assertEqual(out, [self.success])

    def test_isLessThan(self):
        chk = Check(self.spark) \
                   .isLessThan('_5', '_2', lambda x: x == 0.375)
        out = self.suite.onData(self.df).addCheck(chk).run()
        out = DataFrame(out, self.spark).select('constraint_status').collect()
        self.assertEqual(out, [self.success])

    def test_isLessThanOrEqualTo(self):
        chk = Check(self.spark) \
                   .isLessThanOrEqualTo('_5', '_2', lambda x: x == 0.375)
        out = self.suite.onData(self.df).addCheck(chk).run()
        out = DataFrame(out, self.spark).select('constraint_status').collect()
        self.assertEqual(out, [self.success])

    def test_isGreaterThan(self):
        chk = Check(self.spark) \
                   .isGreaterThan('_5', '_2', lambda x: x == 0.125)
        out = self.suite.onData(self.df).addCheck(chk).run()
        out = DataFrame(out, self.spark).select('constraint_status').collect()
        self.assertEqual(out, [self.success])

    def test_isGreaterThanOrEqualTo(self):
        chk = Check(self.spark) \
                   .isGreaterThanOrEqualTo('_5', '_2', lambda x: x == 0.125)
        out = self.suite.onData(self.df).addCheck(chk).run()
        out = DataFrame(out, self.spark).select('constraint_status').collect()
        self.assertEqual(out, [self.success])

                   #.isContainedIn('_3', ['DELAYED', 'INTRANSIT'])

    def test_isInInterval(self):
        chk = Check(self.spark) \
                   .isInInterval('_5', 1.0, 50.0)
        out = self.suite.onData(self.df).addCheck(chk).run()
        out = DataFrame(out, self.spark).select('constraint_status').collect()
        self.assertEqual(out, [self.success])

if __name__ == '__main__':
    unittest.main()
