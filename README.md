# PyDeequ
[![GitHub license](https://img.shields.io/github/license/margitaii/pydeequ.svg)](https://github.com/margitaii/pydeequ/blob/master/LICENSE)
![Code coverage](coverage.svg)

PyDeequ is a python wrapper around [Deequ](https://github.com/awslabs/deequ), which is a "unit testing" framework for data and written in Scala. The main motivation of PyDeequ is to enable data science projects to discover the quality of their input data without leaving the python based environment. Deequ is built on top of [Apache Spark](https://spark.apahce.org), therefore PyDeequ utilizes the [PySpark API](http://spark.apache.org/docs/latest/api/python/index.html) of the Apache Spark project.

The design of PyDeequ API aims to follow the Float API concept of the Deequ Scala project. There are four interfaces of Deequ which are available in PyDeequ BUT in many cases they are implemented only with their default parameters in PyDeequ. In order to improve the API coverage we are happy to receive feedback and [contributions](CONTRIBUTIONS.md) to this project.

For further details on the Deequ API design and examples please refer to the following resources:

 * [Deequ on Github](https://github.com/awslabs/deequ)
 * [Test data quality at scale with Deequ](https://aws.amazon.com/blogs/big-data/test-data-quality-at-scale-with-deequ/)
 * [Research paper of the Deequ package](http://www.vldb.org/pvldb/vol11/p1781-schelter.pdf)

# Requirements

PyDeequ is tested on Spark 2.3.0 and [Deequ 1.0.3-rc2](https://github.com/awslabs/deequ/tree/1.0.3-rc2). You need to initialize you pyspark session with the Deequ jar package. The jar file is available via [maven central](http://mvnrepository.com/artifact/com.amazon.deequ/deequ), download it with

```bash
wget https://repo1.maven.org/maven2/com/amazon/deequ/deequ/1.0.3-rc2/deequ-1.0.3-rc2.jar
```

and start pyspark with

```bash
pyspark --jars ./deequ-1.0.3-rc2.jar
```

# Basic usage

Currently PyDeequ only implements the basic functionality of Deequ but hopefully it still brings some value to a python based data science project. In the followings we demonstrate the basic usage of these four functionalities. There are also example files available in `src\pydeequ\examples`.

The [main components](https://aws.amazon.com/blogs/big-data/test-data-quality-at-scale-with-deequ/) of Deequ are

 * __[Analyzers](src\pydeequ\analyzers.py)__: main metrics computation engine of Deequ, they output __descriptive statistics__ on tabular input data,
 * __[Constrain verification](src\pydeequ\checks.py)__: predefined __data quality checks__ with a threshold values as parameters, they based on metrics computed by the analyzers,
* __[Constrain suggestions](src\pydeequ\suggestions.py)__: automated constrain generation based on a set of rules which profile the data first to come up with useful constrains.

![img](https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2019/05/10/DataDeequ1.png)

There is a test dataset available in the `examples` what we use in the followings to run demos.

```python
>>> from pydeequ.examples import test_data
>>> df = spark.createDataFrame(test_data)
>>> df.show()
+------+----+----------+-----+----+                                             
|    _1|  _2|        _3|   _4|  _5|
+------+----+----------+-----+----+
|thingA|13.0|IN_TRANSIT| true| 5.0|
|thingA| 5.0|   DELAYED|false|20.0|
|thingB|null|   DELAYED| null|12.0|
|thingC|null|IN_TRANSIT|false| 2.0|
|thingD| 1.0|   DELAYED| true|null|
|thingC| 7.0|   UNKNOWN| null|null|
|thingC|20.0|   UNKNOWN| null| 3.5|
|thingE|20.0|   DELAYED|false| 8.2|
+------+----+----------+-----+----+

>>> 
```


## Constrain Verification

The main entry point for the end-user is the *VerificationSuite*. We can add data and __checks__ to the suite following chained method calls. Majority of the checks take column(s) as input and an *assertion* function which takes a metric value as input and outputs a boolean value, indicating whether the constrain met or not. The list of these pre-defined checks are available [here](src/pydeequ/checks.py).

```python
>>> from pydeequ.base import VerificationSuite
>>> from pydeequ.checks import Check
>>> from pyspark.sql import DataFrame
>>> r = (VerificationSuite(spark)
...          .onData(df)
...          .addCheck(Check(spark, 'error', 'examples')
...                    .hasSize(lambda x: x == 8)
...                    .isUnique('_2')
...                    .hasCompleteness('_2', lambda x: x >= 0.75)
...                    .hasCorrelation('_2', '_5', lambda x: abs(x) > 0.5)
...                    )) \
...          .run()
>>> out = DataFrame(r, spark)                                                   
>>> out.show()
+--------+-----------+------------+--------------------+-----------------+--------------------+
|   check|check_level|check_status|          constraint|constraint_status|  constraint_message|
+--------+-----------+------------+--------------------+-----------------+--------------------+
|examples|      Error|       Error|SizeConstraint(Si...|          Success|                    |
|examples|      Error|       Error|UniquenessConstra...|          Failure|Value: 0.66666666...|
|examples|      Error|       Error|CompletenessConst...|          Success|                    |
|examples|      Error|       Error|CorrelationConstr...|          Success|                    |
+--------+-----------+------------+--------------------+-----------------+--------------------+

>>> 
>>> spark.sparkContext._gateway.close()
```
The output of data quality verification is presented as a Spark DataFrame. In the example we performed three checks on the `test_data`:

 * *hasSize*: checks the number of rows in the dataset and raises error if it is NOT equal 8
 * *isUnique*: checks column `_2` values raises error if they are not unique, as only 4 values (13, 5, 1, 7) are unique from the 6 distinct values, it raises an error
 * *hasCompleteness*: checks whether at least 75% of the column `_2` values are populated
 * *hasCorrelation*: checks the correlation between two numerical columns

More examples on verification checks can be found here:
```python
python -m pydeequ.examples.basic_usage
```
__Note__ that the *VerificationSuite* starts a JVM callback server what we need to close at the end of the session.

## Metrics Calculation

We can calculate some statistics on the dataset with the *AnalysisRunner* API. As you see on the architecture diagram above the metrics are the atomic elements of the data "unit testing" in Deequ. __NOTE__: Metrics can be stored in repositories but currently this API is not implemented in PyDeequ.

```python
>>> from pyspark.sql import DataFrame
>>> from pydeequ.base import AnalysisRunner
>>> import pydeequ.analyzers as analyzers
>>> r = (AnalysisRunner(spark)
...     .onData(df)
...     .addAnalyzer(analyzers.Size())
...     .addAnalyzer(analyzers.Completeness('_3'))
...     .addAnalyzer(analyzers.ApproxCountDistinct('_1'))
...     .addAnalyzer(analyzers.Mean('_2'))
...     .addAnalyzer(analyzers.Compliance('top values', '_2 > 15'))
...     .addAnalyzer(analyzers.Correlation('_2', '_5'))) \
...     .run()
>>> out = DataFrame(r, spark)
>>> out.show()
+-----------+----------+-------------------+-------------------+
|     entity|  instance|               name|              value|
+-----------+----------+-------------------+-------------------+
|     Column|        _1|ApproxCountDistinct|                5.0|
|    Dataset|         *|               Size|                8.0|
|     Column|        _3|       Completeness|                1.0|
|Mutlicolumn|     _2,_5|        Correlation|-0.8310775272166866|
|     Column|top values|         Compliance|               0.25|
|     Column|        _2|               Mean|               11.0|
+-----------+----------+-------------------+-------------------+

>>>
```
Metrics are presented as a Spark DataFrame in PyDeequ.

## Data Profiling

Provides column statistics depending on the datatype:

```python
>>> import json 
>>> from pydeequ.profiler import ColumnProfilerRunner
>>> r = (ColumnProfilerRunner()
...      .onData(df)
...      .run())
>>> parsed = json.loads(r)
>>> print(json.dumps(parsed, indent = 4))
{
    "columns": [
        {
            "column": "_3",
            "dataType": "String",
            "isDataTypeInferred": "true",
            "completeness": 1.0,
            "approximateNumDistinctValues": 3,
            "histogram": [
                {
                    "value": "DELAYED",
                    "count": 4,
                    "ratio": 0.5
                },
                {
                    "value": "UNKNOWN",
                    "count": 2,
                    "ratio": 0.25
                },
                {
                    "value": "IN_TRANSIT",
                    "count": 2,
                    "ratio": 0.25
                }
            ]
        },
        {
            "column": "_2",
[...]
 ```
The output of the column profiling is presented in JSON format.

## Constrain Suggestion

It might be a time consuming to define relevant data quality constraints during the data engineering process. The automated constrain suggestion in Deequ provide a useful tool to speed up this process.

```python
>>> import json
>>> from pydeequ.suggestions import ConstraintSuggestionRunner, Rules
>>> r = (ConstraintSuggestionRunner(spark)
...      .onData(df)
...      .addConstraintRule(Rules.CategoricalRangeRule(spark))
...      .run())
>>> parsed = json.loads(r)
>>> print(json.dumps(parsed, indent = 4))
{
    "constraint_suggestions": [
        {
            "constraint_name": "ComplianceConstraint(Compliance('_3' has value range 'DELAYED', 'IN_TRANSIT', 'UNKNOWN',`_3` IN ('DELAYED', 'IN_TRANSIT', 'UNKNOWN')
,None))",
            "column_name": "_3",
            "current_value": "Compliance: 1",
            "description": "'_3' has value range 'DELAYED', 'IN_TRANSIT', 'UNKNOWN'",
            "suggesting_rule": "CategoricalRangeRule()",
            "rule_description": "If we see a categorical range for a column, we suggest an IS IN (...) constraint",
            "code_for_constraint": ".isContainedIn(\"_3\", Array(\"DELAYED\", \"IN_TRANSIT\", \"UNKNOWN\"))"
        }
    ]
}
>>> 
```
The output is presented in JSON format. Constrain suggestions are based on constrain rules, the set of available rules can be found [here](src/pydeequ/suggestions.py).

# License

The PyDeequ library is licensed under the Apache 2.0 license.