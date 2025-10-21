import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, window, date_format, lit, current_date, concat, to_timestamp, when, format_number
from pyspark.sql.types import StructType, StructField, StringType, FloatType

def main():
    with open("config/config.yaml", "r") as f:
        config = yaml.safe_load(f)

    team_number = config["team_number"]
    net_threshold = config["alert_thresholds"]["net_in"]
    disk_threshold = config["alert_thresholds"]["disk_io"]
    window_duration = config["spark_jobs"]["window_duration"]
    slide_duration = config["spark_jobs"]["slide_duration"]
    output_dir = config["paths"]["output_dir"]

    spark = SparkSession.builder \
        .appName(f"Team{team_number}-Network-Disk-Analysis") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    net_schema = StructType([
        StructField("ts", StringType(), True),
        StructField("server_id", StringType(), True),
        StructField("net_in", FloatType(), True)
    ])

    disk_schema = StructType([
        StructField("ts", StringType(), True),
        StructField("server_id", StringType(), True),
        StructField("disk_io", FloatType(), True)
    ])

    # Assuming consumers write to the output directory
    net_data_path = f"{output_dir}net_data.csv"
    disk_data_path = f"{output_dir}disk_data.csv"

    # We need to handle potential schema differences if net_out is present
    # Reading all columns as string first, then selecting and casting
    raw_net_df = spark.read.csv(net_data_path, header=True)
    net_df = raw_net_df.select(
        col("ts"),
        col("server_id"),
        col("net_in").cast(FloatType())
    )

    disk_df = spark.read.csv(disk_data_path, header=True, schema=disk_schema)

    # Add current date to timestamp and convert to timestamp type
    current_day = date_format(current_date(), "yyyy-MM-dd")

    net_df = net_df.withColumn("timestamp", to_timestamp(concat(lit(current_day), lit(" "), col("ts"))))
    disk_df = disk_df.withColumn("timestamp", to_timestamp(concat(lit(current_day), lit(" "), col("ts"))))

    df = net_df.join(disk_df, ["timestamp", "server_id"])

    windowed_df = df.groupBy(
        window(col("timestamp"), window_duration, slide_duration),
        col("server_id")
    ).agg(
        max("net_in").alias("max_net_in"),
        max("disk_io").alias("max_disk_io")
    )

    alert_df = windowed_df.withColumn(
        "alert",
        when((col("max_net_in") > net_threshold) & (col("max_disk_io") > disk_threshold), "Network flood + Disk thrash suspected")
        .when((col("max_net_in") > net_threshold) & (col("max_disk_io") <= disk_threshold), "Possible DDoS")
        .when((col("max_disk_io") > disk_threshold) & (col("max_net_in") <= net_threshold), "Disk thrash suspected")
        .otherwise("Normal")
    )

    final_df = alert_df.select(
        col("server_id"),
        date_format(col("window.start"), "HH:mm:ss").alias("window_start"),
        date_format(col("window.end"), "HH:mm:ss").alias("window_end"),
        format_number(col("max_net_in"), 2).alias("max_net_in"),
        format_number(col("max_disk_io"), 2).alias("max_disk_io"),
        col("alert")
    ).filter(col("alert") != "Normal")

    output_path = f"{output_dir}team_{team_number}_NET_DISK"
    final_df.write.csv(output_path, header=True, mode="overwrite")

    print(f"Spark Job 2 finished. Output written to {output_path}")

    spark.stop()

if __name__ == "__main__":
    main()
