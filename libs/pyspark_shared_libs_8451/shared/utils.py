from pyspark.sql.functions import col, avg, lit, stddev_pop
from pyspark.sql.types import DoubleType
import uuid

def standardize(df):
    # get a list of columns to standardized
    standardize_cols = list(filter(lambda c: c != "ehhn", df.columns))

    # create a list of expressions to average/stddev each column
    avg_columns = map(lambda c: avg(col(c).cast(DoubleType())).alias(c), standardize_cols)
    stddev_columns = map(lambda c: stddev_pop(col(c).cast(DoubleType())).alias(c), standardize_cols)

    # create a dataframe of the averages/stddevs
    df_avgs = df.groupBy().agg(*avg_columns).na.fill(0)
    df_stddevs = df.groupBy().agg(*stddev_columns).na.fill(0)
    # df_avgs.select('population_density', 'cafeteria_count', 'casual_dining_count', 'family_restaurant_count').show()

    avgs = df_avgs.first().asDict()
    stddevs = df_stddevs.first().asDict()

    exprs = [col("ehhn")] + list(
        map(lambda c: lit(0).alias(c) if (stddevs[c] == 0) else ((col(c) - avgs[c]) / stddevs[c]).alias(c),
            standardize_cols))

    return df[exprs]


def zscore(c, c_avg, c_stddev):
    if c_stddev == 0:
        return lit(0).alias(c)
    else:
        return ((col(c) - c_avg) / c_stddev).alias(c)


def non_lambda_standardize(df):
    # get a list of columns to standardized
    standardize_cols = [colName for colName in df.columns if colName != "ehhn"]

    # create a list of expressions to average/stddev each column
    avg_columns = [avg(col(colName)).alias(colName) for colName in standardize_cols]
    stddev_columns = [stddev_pop(col(col_name)).alias(col_name) for col_name in standardize_cols]

    # create a dataframe of the averages/stddevs
    df_avgs = df[avg_columns].na.fill(0)
    df_stddevs = df[stddev_columns].na.fill(0)
    # df_avgs.select('population_density', 'cafeteria_count', 'casual_dining_count', 'family_restaurant_count').show()

    avgs = df_avgs.first().asDict()
    stddevs = df_stddevs.first().asDict()

    exprs = [zscore(colName, avgs[colName][0], stddevs[colName][0]) for colName in standardize_cols]
    exprs.insert(0, col("ehhn"))

    return df[exprs]


def rescale_dataframe(spark, df, source_column, target_column, to_min_value, to_max_value, from_min_value, from_max_value):
    # todo: Test that column is numeric?
    # todo: Test that the ranges are right-side up?

    # What is the offset to get the original min to the new min
    intercept = to_min_value - from_min_value
    # Conversion factor is the newMax / (old Max + the intercept)
    slope = to_max_value / (from_max_value + intercept)

    temp_table_name = "temp_rescale_" + uuid.uuid4().hex
    df.createOrReplaceTempView(temp_table_name)

    sql_clause = "Select *, (" + source_column + " + " + str(intercept) + ") * " + str(slope) + " AS " + target_column + " from " + temp_table_name
    ret_df = spark.sql(sql_clause)

    return ret_df


def filterProducts(product, filter_sub_commodity=True):
    """
    :param product:
    :param filter_sub_commodity:
    :return:
    """
    '''
    filter out the following departments/commodities:
      - rec departments supplies (30), misc sales tran (34), fuel (58), bottle dep/ret (78)
      - sub department pharmacy (06)
      - sub commodity non ppd phones (83434)
    '''
    recap_dept = ['30', '34', '58', '78']
    sub_dept = ['06']
    sub_com = ['83434']

    lower_product = product.toDF(*[c.lower() for c in product.columns])

    if filter_sub_commodity:
        return lower_product \
                .filter(~lower_product["pid_fyt_rec_dpt_cd"].isin(*recap_dept)) \
                .filter(~lower_product["pid_fyt_sub_dpt_cd"].isin(*sub_dept)) \
                .filter(~lower_product["pid_fyt_sub_com_cd"].isin(*sub_com))
    else:
        return lower_product \
                .filter(~lower_product["pid_fyt_rec_dpt_cd"].isin(*recap_dept)) \
                .filter(~lower_product["pid_fyt_sub_dpt_cd"].isin(*sub_dept))
