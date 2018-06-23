from pyspark.sql import SparkSession
from .filewrapper import FileWrapper
from ..metadata.metalogger import MetaLogger
from resources.config import app_config

class Initializer:
    def __init__(self, job_name, spark=None, env='default'):
        if not spark:
            self.spark = SparkSession.builder.appName(job_name).getOrCreate()
        else:
            self.spark = spark
        self.sc = self.spark.sparkContext

        # Set up log4j logger
        self.log4jLogger = self.sc._jvm.org.apache.log4j
        self.LOGGER = self.log4jLogger.LogManager.getLogger(self.__class__.__name__)
        kafkaLogger = self.log4jLogger.LogManager.getLogger('kafkaLogger')
        self.metaLogger = MetaLogger(kafkaLogger, run_name=job_name, spark_user=self.sc.sparkUser())

        # same call as you'd make in java, just using the py4j methods to do so
        self.LOGGER.setLevel(self.log4jLogger.Level.INFO)
        self.LOGGER.info("pyspark script logger initialized")

        # exclude noise from non 84.51 code
        self.log4jLogger.LogManager.getLogger("org"). \
            setLevel(self.log4jLogger.Level.ERROR)
        self.log4jLogger.LogManager.getLogger("akka"). \
            setLevel(self.log4jLogger.Level.ERROR)
        self.log4jLogger.LogManager.getLogger("spark"). \
            setLevel(self.log4jLogger.Level.ERROR)

        # set up custom file wrapper
        self.fileWrapper = FileWrapper(self.spark, self.LOGGER, self.metaLogger)

        self.config = app_config[env]