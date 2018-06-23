from pyspark.sql import SparkSession
from .dictmerge import dict_merge
from ..metadata.metalogger import MetaLogger
from .filewrapper import FileWrapper


class ApplicationContext(object):
    def __init__(self, app_options={}):
        job_name = app_options.get('job_name', 'An_8451_PySpark_job')

        #################################################
        # Spark objects
        #################################################
        self.spark = app_options.get('spark', self._create_spark_session(job_name))
        self.sc = self.spark.sparkContext

        #################################################
        # Loggers
        #################################################
        '''
        this should be an internal property
        '''
        self._log4j_logger = self.sc._jvm.org.apache.log4j

        self.LOGGER = self._log4j_logger.LogManager.getLogger(job_name)
        # same call as you'd make in java, just using the py4j methods to do so
        self.LOGGER.setLevel(self._log4j_logger.Level.INFO)
        self.LOGGER.info("pyspark script logger initialized")
        # exclude noise from non 84.51 code
        self._log4j_logger.LogManager.getLogger("org"). \
            setLevel(self._log4j_logger.Level.ERROR)
        self._log4j_logger.LogManager.getLogger("akka"). \
            setLevel(self._log4j_logger.Level.ERROR)
        self._log4j_logger.LogManager.getLogger("spark"). \
            setLevel(self._log4j_logger.Level.ERROR)

        #################################################
        # App config object
        #################################################
        app_config = app_options.get('app_config', {})
        default_cfg = app_config.get('default', {})
        env = app_options.get('env', 'default')
        env_cfg = app_config.get(env, {})
        self.config = dict_merge(default_cfg, env_cfg)

        #################################################
        # Metadata Logger
        #################################################
        meta_logger_enabled = app_options.get('meta_logger_enabled', False)
        file_wrapper_enabled = app_options.get('io_enabled', False)
        if meta_logger_enabled or file_wrapper_enabled:
            ''' Since built-in file_wrapper needs metadata logger, one should be created if io_enabled == True
            '''
            if 'meta_logger' in app_options:
                ''' A custom meta_logger is provided by caller
                '''
                self.metaLogger = app_options['meta_logger']
            else:
                ''' providing a built-in meta-logger
                '''
                kafka_logger = self._log4j_logger.LogManager.getLogger('kafkaLogger')
                self.metaLogger = MetaLogger(kafka_logger,
                                             run_name=job_name,
                                             spark_user=self.sc.sparkUser(),
                                             git_info=app_options.get('git_info', {}))

            self._decorate_spark_session_stop(self.metaLogger)

        #################################################
        # File wrapper
        #################################################
        if file_wrapper_enabled:
            if 'io' in app_options:
                ''' A custom file_wrapper is provided by caller 
                '''
                self.io = app_options['io']
            else:
                ''' providing a built-in file wrapper
                '''
                self.io = FileWrapper(self.spark, self.LOGGER, self.metaLogger)

    @staticmethod
    def _create_spark_session(job_name):
        return SparkSession.builder.appName(job_name).getOrCreate()

    @staticmethod
    def _decorate_spark_session_stop(meta_logger=None):
        def new_stop(self):
            if self._meta_logger is not None:
                self._meta_logger.done()
            self._original_stop()

        SparkSession._meta_logger = meta_logger
        SparkSession._original_stop = SparkSession.stop
        SparkSession.stop = new_stop

