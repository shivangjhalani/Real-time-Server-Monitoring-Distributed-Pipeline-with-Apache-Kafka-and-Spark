import sys
import yaml
import os

from producer.producer import run_producer
from consumers.consumer1 import run_consumer1
from consumers.consumer2 import run_consumer2
from spark_jobs.spark_job1 import process_cpu_mem_data
from spark_jobs.spark_job2 import process_net_disk_data
from pyspark.sql import SparkSession

def main():
    if len(sys.argv) < 2:
        print("Usage: python main.py [producer|consumer1|consumer2|spark-job1|spark-job2] [--fast]")
        sys.exit(1)

    role = sys.argv[1]
    fast_mode = '--fast' in sys.argv

    with open('config/config.yaml', 'r') as f:
        config = yaml.safe_load(f)

    if not os.path.exists(config['paths']['output_dir']):
        os.makedirs(config['paths']['output_dir'])

    if role == "producer":
        run_producer(config, fast_mode)
    elif role == "consumer1":
        run_consumer1(config)
    elif role == "consumer2":
        run_consumer2(config)
    elif role == "spark-job1":
        spark = SparkSession.builder.appName("CPU_Memory_Analysis").getOrCreate()
        process_cpu_mem_data(spark, config)
        spark.stop()
    elif role == "spark-job2":
        spark = SparkSession.builder.appName("Network_Disk_Analysis").getOrCreate()
        process_net_disk_data(spark, config)
        spark.stop()
    else:
        print(f"Unknown role: {role}")
        sys.exit(1)

if __name__ == "__main__":
    main()
