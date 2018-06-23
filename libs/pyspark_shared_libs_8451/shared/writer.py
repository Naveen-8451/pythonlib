class FileWriter:
    def __init__(self, logger, meta_logger):
        self.logger = logger
        self.meta_logger = meta_logger
        self.printSchema = False

    def parquet(self, df, path):
        self.logger.info('Writing parquet to {}'.format(path))
        self.meta_logger.logOutput(path)
        df.write.mode('overwrite').parquet(path)

    def csv(self, df, path):
        self.logger.info('Writing csv to {}'.format(path))
        self.meta_logger.logOutput(path)
        df.write.mode('overwrite').csv(path)
