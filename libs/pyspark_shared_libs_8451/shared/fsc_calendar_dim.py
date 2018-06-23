#!/opt/anaconda3/bin/python3
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import col
from enum import Enum
from datetime import datetime, timedelta


class DateKeyKind(Enum):
    # YYYYPPWW
    period_week = 1
    # YYYYMMDD
    calendar = 2
    # ### - This is very dangerous as the epoch calendar date may some day change
    #       This is used internally to add/subtract weeks
    ordinal = 3


class FiscalCalendar:
    def __init__(self, ctx):
        global logger
        logger = ctx.LOGGER
        spark = ctx.spark
        config = ctx.config

        '''
        Should we load a dictionary or just keep reading file for subsequent runs?
        Most callers will only need to call this once, so I feel like a dictionary/map is overkill
        '''

        # TODO: Should this be configurable? I feel like the location is static
        calendar_file = config["calendar_file"]  # "/data/mart/ild/prd/ild_dm/lkups/date_fis_week_lkp.txt"

        # stuff the kroger fiscal calendar into a dataframe
        global df_fiscal_weeks
        df_fiscal_weeks = spark.read.format("csv"). \
            option("header", "true"). \
            option("delimiter", "|"). \
            option("mode", "DROPMALFORMED"). \
            load(calendar_file). \
            withColumn("rownum", monotonically_increasing_id()). \
            select("rownum", "FIS_WEEK_ID", "FIS_WEEK_START_DATE", "FIS_WEEK_END_DATE"). \
            orderBy(col("FIS_WEEK_ID"))

    @classmethod
    def get_week(cls, ctx, datekey, kind=DateKeyKind.period_week, minus=0):
        cls(ctx)
        if int(minus) == 0:
            # return the week the periodweek
            if kind == DateKeyKind.period_week:
                return df_fiscal_weeks. \
                    filter(col("FIS_WEEK_ID") == datekey). \
                    first()

            # return the week the calendar date
            if kind == DateKeyKind.calendar:
                return df_fiscal_weeks. \
                    filter(col("FIS_WEEK_START_DATE") <= datekey). \
                    filter(col("FIS_WEEK_END_DATE") >= datekey). \
                    first()

            # return the week the id
            if kind == DateKeyKind.ordinal:
                return df_fiscal_weeks. \
                    filter(col("rownum") == datekey). \
                    first()

        # find the id of the week
        week_id = FiscalCalendar.get_week(ctx, datekey, kind).asDict()["rownum"]
        return FiscalCalendar.get_week(ctx, week_id - int(minus), DateKeyKind.ordinal)

    @classmethod
    def get_start_date(cls, ctx, datekey, kind=DateKeyKind.period_week, minus=0):
        cls(ctx)
        return FiscalCalendar.get_week(ctx, datekey, kind, minus). \
            asDict()["FIS_WEEK_START_DATE"]

    # return the day after the week identified by the datekey as the last date in the range
    #        and the get_start_date(*args) for the first date in the range
    #        Callers filter should then be >= startDate and < endDate
    @classmethod
    def get_date_range(cls, ctx, datekey, kind=DateKeyKind.period_week, minus=0):
        cls(ctx)
        start_date = FiscalCalendar.get_week(ctx, datekey, kind, minus). \
            asDict()["FIS_WEEK_START_DATE"]
        end_date = FiscalCalendar.get_week(ctx, datekey, kind, 0). \
            asDict()["FIS_WEEK_END_DATE"]
        next_date = datetime.strftime(datetime.strptime(end_date, '%Y%m%d') + timedelta(days=1), '%Y%m%d')
        return {'start_date': start_date, 'end_date': end_date, 'day_after_end_date': next_date}

    @classmethod
    def get_calendar_date_list(cls, ctx, datekey, kind=DateKeyKind.period_week, lookback=1):
        cls(ctx)
        start_date = datekey \
            if kind == DateKeyKind.calendar \
            else cls.get_start_date(ctx, datekey, kind)
        date_list = [start_date]
        next_date = start_date
        step = -1 if lookback == 1 else 1
        for x in range(1, 90):
            next_date = datetime.strftime(datetime.strptime(next_date, '%Y%m%d') + timedelta(days=step), '%Y%m%d')
            date_list.append(next_date)

        return date_list

    @classmethod
    def get_period_week_list(cls, ctx, datekey, kind=DateKeyKind.period_week, lookback=1):
        cls(ctx)
        week = FiscalCalendar.get_week(ctx, datekey, kind)

        df_periods = df_fiscal_weeks. \
            filter("FIS_WEEK_ID" >= week["FIS_WEEK_ID"]). \
            select(col("FIS_WEEK_ID").alias("period_week")). \
            groupBy("FIS_WEEK_ID"). \
            orderBy(col("FIS_WEEK_ID")) \
            if lookback == 1 else \
            df_fiscal_weeks. \
                filter("FIS_WEEK_ID" <= week["FIS_WEEK_ID"]). \
                select(col("FIS_WEEK_ID").alias("period_week")). \
                groupBy("FIS_WEEK_ID"). \
                orderBy(col("FIS_WEEK_ID").desc)

        #turn data frame into a list
        return df_periods.collect()
