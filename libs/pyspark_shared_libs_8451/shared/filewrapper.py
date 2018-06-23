from .reader import FileReader
from .writer import FileWriter


class FileWrapper:
    def __init__(self, spark, logger, meta_logger):
        self.__spark = spark
        self.__logger = logger
        self.__meta_logger = meta_logger

    @property
    def read(self):
        return FileReader(self.__spark, self.__logger, self.__meta_logger)

    @property
    def write(self):
        return FileWriter(self.__logger, self.__meta_logger)
