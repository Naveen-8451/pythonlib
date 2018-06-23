class FileReader:
    def __init__(self, spark, logger, meta_logger):
        self.spark = spark
        self.logger = logger
        self.meta_logger = meta_logger
        self.printSchema = False
        self.header = "false"
        self.delimiter = ","
        self.mode = "PERMISSIVE"

    def parquet(self, path):
        print("Reading parquet file from " + path)
        self.logger.info("Reading parquet file from " + path)
        self.meta_logger.logInput(path, 'parquet file')
        df = self.spark.read.parquet(path)

        if self.printSchema:
            print("Parquet schema: " + df.schema.treeString)
            self.logger.info("Parquet schema: " + df.schema.treeString)

        return df

    def csv(self, path, **remaining_arguments):
        print("Reading csv file from " + path)
        self.logger.info("Reading csv file from " + path)
        self.meta_logger.logInput(path, 'csv file')
        df = self.spark.read.option("header", self.header).option("delimiter", self.delimiter). \
            option("mode", self.mode).csv(path, **remaining_arguments)

        if self.printSchema:
            print("Parquet schema: " + df.schema.treeString)
            self.logger.info("Parquet schema: " + df.schema.treeString)

        return df

    def option(self, key, value):
        if key == "printSchema":
            self.printSchema = value
        if key == "header":
            self.header = value
        if key == "delimiter":
            self.delimiter = value
        if key == "mode":
            self.mode = value
        return self
