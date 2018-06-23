'''
The goal here is to create a mechanism to walk back or forward
in time to find the most time appropriate file to use as an input
'''

from .fsc_calendar_dim import FiscalCalendar, DateKeyKind
import os
import re
import subprocess


def run_cmd(args_list):
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    proc.communicate()
    return proc.returncode


def file_exists(filepath):
    cmd = ['hdfs', 'dfs', '-test', '-e', filepath]
    ret = run_cmd(cmd)
    return ret == 0


def get_file_list(folder, mask=None):
    cmd = 'hdfs dfs -ls {0}'.format(folder).split()
    noisy_listing = subprocess.check_output(cmd). \
        strip().decode().split('\n')
    file_list = [line.rsplit(None, 1)[-1].lower()  # .replace(folder, '')
                 for line in noisy_listing if len(line.rsplit(None, 1))][1:]

    filter_mask = ("" if mask is None else mask).lower()

    return [file for file in file_list if filter_mask in file]


def get_date_from_filename(filename):
    # Regex pattern for 8 consecutive digits (20160101)
    cal_date_pattern = r"""(\d\d\d\d\d\d\d\d)"""
    match = None
    for match in re.finditer(cal_date_pattern, filename):
        pass
    if match is not None:
        return match.group(0)

    return get_iri_date_from_filename(filename)


def get_iri_date_from_filename(filename):
    iri_match = None
    iri_date_pattern = r"""(\d\d\d\d_\d\d_\d\d)"""
    for iri_match in re.finditer(iri_date_pattern, filename):
        pass
    if iri_match is not None:
        return iri_match.group(0).replace("_", "")

    # "Date could not be found in {0}".format( filename)
    return None


class FileFinder:
    def __init__(self, ctx):
        global logger
        logger = ctx.LOGGER

    '''
    Get a list of files in the directory [file_mask]
    Sort by the getFileDate().desc for walk_back
    Sort by the getFileDate() for walk_forward
    return the first value found
    '''

    @classmethod
    def walk_back(cls, ctx, folder, file_mask, datekey,
                  input_kind=DateKeyKind.calendar,
                  return_kind=DateKeyKind.calendar):
        cls(ctx)  # may be too married to a pattern

        datekey = datekey if input_kind == return_kind \
            else FiscalCalendar.get_week(ctx, datekey, input_kind).toDict()["FIS_WEEK_ID"] \
            if return_kind == DateKeyKind.period_week \
            else FiscalCalendar.get_start_date(ctx, datekey, input_kind)

        files = get_file_list(folder, file_mask)
        # we're walking back so the file must be <= to datekey
        dict_file = {}
        for file in files:
            if get_date_from_filename(file) <= datekey:
                dict_file.update({get_date_from_filename(file): file})
        for key in sorted(dict_file.keys(), reverse=True):
            return dict_file.get(key)

        return None

    @classmethod
    def walk_forward(cls, ctx, folder, file_mask, datekey,
                     input_kind=DateKeyKind.calendar,
                     return_kind=DateKeyKind.calendar):
        cls(ctx)  # may be too married to a pattern

        datekey = datekey if input_kind == return_kind \
            else FiscalCalendar.get_week(ctx, datekey, input_kind).toDict()["FIS_WEEK_ID"] \
            if return_kind == DateKeyKind.period_week \
            else FiscalCalendar.get_start_date(ctx, datekey, input_kind)

        logger.info("Get list of files")
        files = get_file_list(folder, file_mask)
        # we're walking back so the file must be <= to datekey
        dict_file = {}
        for file in files:
            if get_date_from_filename(file) >= datekey:
                dict_file.update({get_date_from_filename(file): file})
        for key in sorted(dict_file.keys()):
            return dict_file.get(key)

        return None
