<!-- 24ada623-5944-4f8d-a3ed-7cc3cfb2796c bbcf76c5-880c-4a0e-9714-e65f240ab7cb -->
# Real-time Server Monitoring Pipeline

This plan outlines the steps to create a complete and modular codebase for a real-time server monitoring pipeline. The solution will be designed to run on four separate machines, each with a specific role, and will use a central `config.yaml` for easy configuration.

## 1. Project Structure

First, I will create the following directory structure to organize the project:

```
server-monitoring/
├── config/
│   └── config.yaml
├── data/
│   └── sample_data.csv
├── output/
├── producer/
│   └── producer.py
├── consumers/
│   ├── consumer1.py
│   └── consumer2.py
├── spark-jobs/
│   ├── spark_job1.py
│   └── spark_job2.py
├── main.py
└── requirements.txt
```

## 2. Configuration (`config.yaml`)

I will create a central configuration file to manage all the settings for the pipeline. This file will include:

- IP addresses for the producer, broker, and consumers.
- Kafka topic names.
- The team number for naming the output files.
- Alerting thresholds for CPU, memory, network, and disk.

## 3. Sample Data (`sample_data.csv`)

I will generate a small sample dataset that adheres to the specified schema: `ts,server_id,cpu_pct,mem_pct,net_in,net_out,disk_io`.

## 4. Producer (`producer.py`)

The producer script will be responsible for:

- Reading the data from `sample_data.csv`.
- Connecting to the Kafka broker.
- Publishing each metric to its corresponding Kafka topic.

## 5. Consumers (`consumer1.py` and `consumer2.py`)

I will create two consumer scripts:

- `consumer1.py`: Subscribes to the `topic-cpu` and `topic-mem` topics and saves the data to `output/cpu_data.csv` and `output/mem_data.csv`.
- `consumer2.py`: Subscribes to the `topic-net` and `topic-disk` topics and saves the data to `output/net_data.csv` and `output/disk_data.csv`.

## 6. Spark Jobs (`spark_job1.py` and `spark_job2.py`)

I will implement two PySpark jobs for data analysis:

- `spark_job1.py`:
    - Reads the CPU and memory data from the CSV files.
    - Performs a sliding window aggregation (30s window, 10s slide).
    - Applies the alerting logic and saves the results to `output/team_{team_number}_CPU_MEM.csv`.
- `spark_job2.py`:
    - Reads the network and disk data from the CSV files.
    - Performs a sliding window aggregation.
    - Applies the alerting logic and saves the results to `output/team_{team_number}_NET_DISK.csv`.

## 7. Main Script (`main.py`)

I will create a `main.py` file to run the different components of the pipeline. This script will take a command-line argument to specify the role of the machine (e.g., `producer`, `consumer1`, `spark-job2`).

## 8. Dependencies (`requirements.txt`)

Finally, I will create a `requirements.txt` file listing all the necessary Python libraries, such as `kafka-python`, `pyspark`, and `pyyaml`.