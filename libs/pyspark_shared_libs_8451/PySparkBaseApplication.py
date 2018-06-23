import argparse
import importlib
from .shared.app_context import ApplicationContext


NO_JOB_NAME = 'job_name_missing'


def run(options={}):
    """ Main entry of all Spark App
    options: provides application runtime options.
    options:
        - args: dict/function.
        - args: optional.  If it is missing, the built-in get_arguments will be used.
            if a function is provided, it will be invoked to return a dict with 3 properties
            - job
            - job_args
            - env
        - job_name: name of job module (file name under src/jobs).  If missing, application will try to obtain it from
            command line arguments
        - job_args: Array of values passed into job main() function
        - env: optional: environment name: default/development/test/local.  Default default
        - spark: optional. SparkContext object.
        - app_config: optional.  Entire application configuration dict object (not just 1 section)
        - meta_logger_enabled: optional. Boolean.  default to False.  If set to True, a metaLogger object will be
            assigned to ApplicationContext object
        - meta_logger: optional.  An externally provided metadata logger object.  A built-in will be provided if
            meta_logger is missing and meta_logger_enabled = True
        - io_enabled: optional. Boolean.  default to False.  If set to True. a io object will be
            assigned to ApplicationContext object
        - io: optional.  An externally provided file wrapper.  A built-in will be provided if file_wrapper
            is missing and io_enabled = true
        - git_info: optional.  A dict to provide git repo info for meta data logger.
            {
                git_url,
                git_branch,
                git_commit
            }
    """

    options = _prepare_options(options)

    if options.get('job_name', NO_JOB_NAME) != NO_JOB_NAME:
        _execute_module(options)
    else:
        print('No job name specified, exiting.')


def _execute_module(options):
    ctx = ApplicationContext(options)
    # Importing our job script
    job_module = importlib.import_module('src.jobs.{}'.format(options.get('job_name')))
    # Calling the main method of our job
    job_module.main(ctx, *options.get('job_args'))


def _get_arguments():
    """ Private function to read arguments list from command line """
    # Argparser is a python library for dealing with command line args
    parser = argparse.ArgumentParser()

    # This is what job will be ran. Should be a directory under /jobs
    parser.add_argument('--job', type=str, required=True)

    # This is for any extra arguments coming through spark-submit
    parser.add_argument('--job_args', nargs='*', default=[])

    # This is for section in configuration
    parser.add_argument('--env', type=str, required=False, default='default')

    args = parser.parse_args()

    return {
        'job': args.job,
        'job_args': args.job_args,
        'env': args.env
    }


def _prepare_options(options={}):
    options = {} if options is None else options

    options['args'] = options.get('args', _get_arguments)
    if callable(options['args']):
        args = options['args']()
    else:
        args = options.get('args')
    args = args if args is not None else {}

    options['job_name'] = args.get('job', NO_JOB_NAME)
    options['job_name'] = NO_JOB_NAME if not options['job_name'] else options['job_name']
    options['job_args'] = args.get('job_args', [])
    options['env'] = args.get('env', 'default') if args.get('env', 'default') != 'default' else options.get('env', 'default')
    options['meta_logger_enabled'] = options.get('meta_logger_enabled', True)
    options['io_enabled'] = options.get('io_enabled', True)
    options['git_info'] = options.get('git_info', {})
    return options
