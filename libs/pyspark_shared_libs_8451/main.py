import argparse
import importlib
from .shared.initializer import Initializer

# Argparser is a python library for dealing with command line args
parser = argparse.ArgumentParser()

# This is what job will be ran. Should be a directory under /jobs
parser.add_argument('--job', type=str, required=True)

# This is for any extra arguments coming through spark-submit
parser.add_argument('--job_args', nargs='*', default=[])

# This is for section in configuration
parser.add_argument('--env', type=str, required=False, default='default')

args = parser.parse_args()

# Importing our job script

job_module = importlib.import_module('src.jobs.{}'.format(args.job))

ctx = Initializer(args.job, spark=None, env=args.env)

# Calling the main method of our job
job_module.main(ctx, *args.job_args)