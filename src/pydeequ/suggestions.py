import py4j.java_gateway as jg
import pdb

from pydeequ.exceptions import JavaClassNotFoundException
import pydeequ.jvm_conversions as jc

class ConstraintSuggestionRunBuilder:
    """
    A class to build a ConstraintSuggestionRun using a fluent API.
    """
    def __init__(self, SparkSession, dataFrame):
        """
        Args:
            SparkSession (pyspark.sql.SparkSession)
            dataFrame (pyspark.sql.dataframe.DataFrame)
        """
        self.spark = SparkSession
        self._dataFrame = dataFrame
        run_builder = self._jvm.com.amazon.deequ.suggestions.ConstraintSuggestionRunBuilder
        self.jvmConstraintSuggestionRunBuilder = run_builder(
            self.dataFrame._jdf
        )

    @property
    def _jsparkSession(self):
        return self.spark._jsparkSession

    @property
    def _jvm(self):
        return self.spark.sparkContext._jvm

    @property
    def dataFrame(self):
        return self._dataFrame

    def addConstraintRule(self, constraint):
        """
        Add a single rule for suggesting constraints based on ColumnProfiles to the run.
        
        Args:
            constraintRule
        """
        jvmRule = constraint._jvmRule
        self.jvmConstraintSuggestionRunBuilder.addConstraintRule(jvmRule())
        return self

    def run(self):
        result = self.jvmConstraintSuggestionRunBuilder.run()

        jvmSuggestionResult = self._jvm.com.amazon.deequ \
            .suggestions.ConstraintSuggestionResult
        df = jvmSuggestionResult.getConstraintSuggestionsAsJson(
            result
        )
        return df

class ConstraintSuggestionRunner:
    """
    """
    def __init__(self, SparkSession):
        """
        Args:
            SparkSession ():
        """
        self.spark = SparkSession

    def onData(self, dataFrame):
        """
        Starting point to construct a run on constraint suggestions.
        
        Args:
            dataFrame (pyspark.sql.dataframe.DataFrame):
            spark dataFrame on which the checks will be verified.
        """
        return ConstraintSuggestionRunBuilder(self.spark, dataFrame)

class Rules:
    """
    Constraint rules
    """

    def __init__(self, spark, _jvmRule):
        self.spark = spark
        self._jvmRule = _jvmRule

    @property
    def _jvm(self):
        return self.spark.sparkContext._jvm

    @classmethod
    def CompleteIfCompleteRule(cls, spark):
        _jvmRule = spark.sparkContext._jvm.com.amazon.deequ.suggestions.rules.CompleteIfCompleteRule
        return cls(spark, _jvmRule)

    @classmethod
    def RetainCompletenessRule(cls, spark):
        _jvmRule = spark.sparkContext._jvm.com.amazon.deequ.suggestions.rules.RetainCompletenessRule
        return cls(spark, _jvmRule)

    @classmethod
    def RetainTypeRule(cls, spark):
        _jvmRule = spark.sparkContext._jvm.com.amazon.deequ.suggestions.rules.RetainTypeRule
        return cls(spark, _jvmRule)

    @classmethod
    def CategoricalRangeRule(cls, spark):
        _jvmRule = spark.sparkContext._jvm.com.amazon.deequ.suggestions.rules.CategoricalRangeRule
        return cls(spark, _jvmRule)

    @classmethod
    def FractionalCategoricalRangeRule(cls, spark):
        _jvmRule = spark.sparkContext._jvm.com.amazon.deequ.suggestions.rules.FractionalCategoricalRangeRule
        return cls(spark, _jvmRule)
    
    @classmethod
    def NonNegativeNumbersRule(cls, spark):
        _jvmRule = spark.sparkContext._jvm.com.amazon.deequ.suggestions.rules.NonNegativeNumbersRule
        return cls(spark, _jvmRule)