# PyDeequ
[![GitHub license](https://img.shields.io/github/license/margitaii/pydeequ.svg)](https://github.com/margitaii/pydeequ/blob/master/LICENSE)

PyDeequ is a python wrapper around [Deequ](https://github.com/awslabs/deequ), which is a "unit testing" framework for data and written in Scala. The main motivation of PyDeequ is to enable data science projects to discover the quality of their input data without leaving the python based environment. Deequ is built on top of Apache Spark, therefore PyDeequ utilizes the PySpark API of the Apache Spark project.

The design of PyDeequ API aims to follow the Float API concept of the Deequ Scala project. There are four interfaces of Deequ which are available in PyDeequ BUT in many cases they are implemented only with their default parameters in PyDeequ. In order to improve the API coverage we are happy to receive feedback and [contributions](CONTRIBUTIONS.md) to this project.

For further details on the Deequ API design and examples please refer to the following resources:

 * [Github](https://github.com/awslabs/deequ)
 * [Test data quality at scale with Deequ](https://aws.amazon.com/blogs/big-data/test-data-quality-at-scale-with-deequ/)
 * [Research paper of the Deequ package](http://www.vldb.org/pvldb/vol11/p1781-schelter.pdf)

# Requirements

PyDeequ is tested on Spark 2.3.0 and [Deequ 1.0.3-rc2](https://github.com/awslabs/deequ/tree/1.0.3-rc2). You need to initialize you pyspark session with the Deequ jar package. The jar file is avaliable via [maven central](http://mvnrepository.com/artifact/com.amazon.deequ/deequ), download it with

```bash
wget https://repo1.maven.org/maven2/com/amazon/deequ/deequ/1.0.3-rc2/deequ-1.0.3-rc2.jar
```

and start pyspark with

```bash
pyspark --jars ./deequ-1.0.3-rc2.jar
```

# Basic usage