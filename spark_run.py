from pyspark.sql import SparkSession, Window
from datetime import datetime


def get_month() -> str:
    if datetime.now().month < 10:
        return '0' + str(datetime.now().month)
    return str(datetime.now().month)


if __name__ == '__main__':
    spark = SparkSession.builder.appName("clickStreamAnalysis") \
        .config("spark.jars",
                "s3://clickstreamanalysis/extra-jars/mysql-connector-java-8.0.29-tidb-1.0.2.jar").getOrCreate()

    df_s3 = spark.read.parquet(
        f"s3://hourlyclickstreamfromconfluent/topics/dir/clickstream/year={datetime.now().year}/month={get_month()}/day=17/*/*.parquet").select(
        "userid", "status", "request")

    df_s3.createOrReplaceTempView("Clickstream")
    df_s3.repartition("status")

    df_rds = spark.read.format("jdbc") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "User") \
        .option("user", "davidxxi21") \
        .option("password", "35xxxv!!!") \
        .option("url", "jdbc:mysql://clickstream.ccorjew9awjd.ap-northeast-2.rds.amazonaws.com:3306/clickstream") \
        .option("partitionColumn", "id") \
        .option("lowerBound", "1000000") \
        .option("upperBound", "10000000") \
        .option("numPartitions", "200") \
        .load()
    df_rds.createOrReplaceTempView("User")

    df_broadcast_join = spark.sql(
        "SELECT /*+ BROADCAST(u) */ (DATEDIFF(CURRENT_DATE, u.birthdate) div 365) div 10 * 10 as age_level, "
        "u.gender as gender, c.status as request_status, c.request AS request_url "
        "FROM Clickstream c join User u on u.id = c.userid ")

    df_broadcast_join.createOrReplaceTempView("base")
    df_broadcast_join.repartition(1500, "age_level", "gender", "status")



    w = Window.partitionBy()


    result_df = spark.sql("SELECT age_level, gender, request_status, request_url, cnt "
                          "FROM ( SELECT age_level, gender, request_status, request_url,count(*) as cnt, "
                          "row_number() over (partition by age_level, gender, request_status order by count(*) desc ) "
                          "as rn FROM base GROUP BY age_level, gender, request_status, request_url) subquery WHERE rn <= 10")

    result_df.repartition(1200, "age_level", "gender", "request_status")

    result_df.write.mode("overwrite").parquet(
        path=f"s3a://clickstreamanalysis/output/{datetime.now().strftime('%Y-%m-%d')}/result.parquet")
