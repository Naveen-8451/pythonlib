from .metadata import *
from .metadatajsonconverter import MetaDataJsonConverter
import json
import time


class MetaLogger:
    def __init__(self, logger, feature_class='', run_name='', mission='Data DNA', spark_user='', git_info={}):
        self.logger = logger
        self._git_info = git_info
        # a job run metadata object is simple a hash table of input, output and column
        self.metaData = dict(
            featureClass=feature_class,
            featureClassInstance=run_name,
            mission=mission,
            sparkUser=spark_user,
            fileLocation='',
            codeRepo=self._get_code_repo(run_name),
            duration=0,
            inputParams=[],
            outputs=[],
            columns=[])
        self._start = time.time()
        self._finished = False

    def setFeatureClass(self, feature_class):
        self.metaData['featureClass'] = feature_class

    def setRunName(self, run_name):
        self.metaData['featureClassInstance']=  run_name

    def setMission(self, mission):
        self.metaData['mission'] = mission

    def setSparkUser(self, spark_user):
        self.metaData['sparkUser'] = spark_user

    def setCodeRepo(self, code_repo):
        self.metaData['codeRepo'] = code_repo

    def logInput(self, val, key):
        print('logging meta input key={} value={}'.format(key, val))
        self.metaData['inputParams'].append(InputMeta(key, val))

    def logOutput(self, output_path, output_type='Parquet'):
        print('logging meta output location={}'.format(output_path))
        self.metaData['outputs'].append(OutputMeta(output_type, output_path))
        self.metaData['fileLocation'] = self.metaData['fileLocation'] + output_path + ', '

    def logColumns(self, *columns):
        print('logging columns')
        self.metaData['columns'].extend(columns)

    def done(self):
        if not self._finished:
            stop = time.time()
            self.metaData['duration'] = round(stop - self._start, 0)
            self.logger.info(json.dumps(self.metaData, cls=MetaDataJsonConverter))
            print(json.dumps(self.metaData, cls=MetaDataJsonConverter))
            self._finished = True

    def _get_code_repo(self, job_name):
        git_info = self._git_info
        filename = str(job_name).replace('.', '/')
        return "{}/blob/{}/src/jobs/{}.py#{}".format(git_info.get('git_url', 'http://github.8451.com/missing'),
                                                     git_info.get('git_branch', 'master'), filename,
                                                     git_info.get('git_commit', ''))
