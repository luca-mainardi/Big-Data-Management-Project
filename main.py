from pyspark import SparkConf, SparkContext, RDD
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import split
# The code below may help you if your pc cannot find the correct python executable.
# Don't use this code on the server!
# import os
# import sys
# os.environ['PYSPARK_PYTHON'] = sys.executable
# os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# TODO: Make sure that if you installed Spark version 3.3.4 (recommended) that you install the same version of
#  PySpark. You can do this by running the following command: pip install pyspark==3.3.4


def get_spark_context(on_server: bool) -> SparkContext:
    spark_conf = SparkConf().setAppName("2AMD15")
    if not on_server:
        spark_conf = spark_conf.setMaster("local[*]")

    spark_context = SparkContext.getOrCreate(spark_conf)

    if on_server:
        # TODO: You may want to change ERROR to WARN to receive more info. For larger data sets, to not set the
        #  log level to anything below WARN, Spark will print too much information.
        spark_context.setLogLevel("ERROR")

    return spark_context


def q0a(spark_context: SparkContext, on_server: bool) -> DataFrame:
    plays_file_path = "plays.txt" if on_server else "plays.txt"

    spark_session = SparkSession(spark_context)

    # TODO: Implement Q0a here by creating a Dataset of DataFrame out of the file at {@code plays_file_path}.
    df = spark_session.read.format("text").load(plays_file_path)
    df = df.select(
        split(df['value'], ',').getItem(0).cast('int').alias('userid'),
        split(df['value'], ',').getItem(1).cast('int').alias('songid'),
        split(df['value'], ',').getItem(2).cast('int').alias('rating')
    )

    return df
    


def q0b(spark_context: SparkContext, on_server: bool) -> RDD:
    plays_file_path = "/plays.txt" if on_server else "plays.txt"

    # TODO: Implement Q0b here by creating an RDD out of the file at {@code plays_file_path}.
    rdd = spark_context.textFile(plays_file_path)
    return rdd


def q1(spark_context: SparkContext, data_frame: DataFrame):
    # TODO: Implement Q1 here
    spark_session = SparkSession(spark_context)
    # Register the DataFrame as a SQL temporary view
    data_frame.createOrReplaceTempView("plays")
    query = "SELECT COUNT(*)                                    \
            FROM (                                              \
                SELECT userid                                   \
                FROM plays                                      \
                WHERE rating IS NOT NULL                        \
                GROUP BY userid                                 \
                HAVING COUNT(rating) >= 100 AND AVG(rating) < 2 \
            ) AS filtered_users"
    sqlDF = spark_session.sql(query)
    print(sqlDF.show())
    return sqlDF


def q2(spark_context: SparkContext, rdd: RDD):
    return


def q3(spark_context: SparkContext, rdd: RDD):
    # TODO: Implement Q3 here
    return


def q4(spark_context: SparkContext, rdd: RDD):
    # TODO: Implement Q4 here
    return


def q5(spark_context: SparkContext, rdd: RDD):
    # TODO: Implement Q5 here
    return


if __name__ == '__main__':
    on_server = False  # TODO: Set this to true if and only if deploying to the server

    spark_context = get_spark_context(on_server)

    data_frame = q0a(spark_context, on_server)

    rdd = q0b(spark_context, on_server)

    q1(spark_context, data_frame)

    # q2(spark_context, rdd)

    # q4(spark_context, rdd)

    # q5(spark_context, rdd)

    spark_context.stop()
