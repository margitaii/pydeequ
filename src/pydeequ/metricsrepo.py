from pyspark.sql import DataFrame

from pydeequ.base import BaseWrapper
import pydeequ.jvm_conversions as jc

class ResultKey(BaseWrapper):
    """ Unique identifier of Analysis result.
    """
    def __init__(self, SparkSession, dataSetDate, tags):
        """
        :param double dataSetDate: A date related to the Analysis result
        :param dict tags: Key-value store of tags
        """
        super().__init__(SparkSession)
        self.dataSetDate = dataSetDate
        self.tags = tags
        result_key = self._jvm.com.amazon.deequ.repository.ResultKey
        self.jvmResultKey = result_key(
            self.dataSetDate,
            jc.dict_to_scala_map(self._jvm, self.tags)
        )

class FileSystemMetricsRepository(BaseWrapper):
    """ FS based repository class
    """
    def __init__(self, SparkSession, path):
        super().__init__(SparkSession)
        self.path = path
        fs_repo = self._jvm.com.amazon.deequ.repository.fs.\
            FileSystemMetricsRepository
        self.jvmMetricsRepo = fs_repo(
            self._jsparkSession,
            self.path
        )

    def save(self, resultKey, analyserContext):
        """ Save Analysis results (metrics).

        :param ResultKey resultKey: unique identifier of Analysis results
        :param AnalyzerContext analyserContext: 
        """
        return self.jvmMetricsRepo.save(
            resultKey.jvmResultKey,
            analyserContext.jvmAnalyzerContext
        )

    def load(self):
        """ Get a builder class to construct a loading query to get
        analysis results
        """
        return FSRepoResultsLoader(self.spark, self.path)

class FSRepoResultsLoader(BaseWrapper):
    def __init__(self, SparkSession, path):
        super().__init__(SparkSession)
        self.path = path
        fs_repo_loader = self._jvm.com.amazon.deequ.repository.fs.\
            FileSystemMetricsRepositoryMultipleResultsLoader
        self.jvmFSMetricsRepoLoader = fs_repo_loader(
            self._jsparkSession,
            self.path
        )

    def withTagValues(self, tagValues):
        self.tagValues = tagValues
        self.jvmFSMetricsRepoLoader = self.jvmFSMetricsRepoLoader \
            .withTagValues(
                jc.dict_to_scala_map(self._jvm, tagValues)
        )
        return self

    def before(self, dateTime):
        self.before = dateTime
        self.jvmFSMetricsRepoLoader = self.jvmFSMetricsRepoLoader \
            .before(
                dateTime
        )
        return self

    def after(self, dateTime):
        self.after = dateTime
        self.jvmFSMetricsRepoLoader = self.jvmFSMetricsRepoLoader \
            .after(
                dateTime
        )
        return self

    def getMetricsAsDF(self):
        jvmGetter = self.jvmFSMetricsRepoLoader.getSuccessMetricsAsDataFrame
        df = jvmGetter(
            self._jsparkSession,
            getattr(self.jvmFSMetricsRepoLoader, 
                "getSuccessMetricsAsDataFrame$default$2")()
        )
        return DataFrame(df, self.spark)

    def getMetricsAsJson(self):
        jvmGetter = self.jvmFSMetricsRepoLoader.getSuccessMetricsAsJson
        jf = jvmGetter(
             getattr(self.jvmFSMetricsRepoLoader,
                "getSuccessMetricsAsJson$default$1")()
        )
        return jf





