import yaml
import csv
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError

def create_producer(broker_ip):
    # Short timeouts so we fail fast if broker is unreachable
    return KafkaProducer(
        bootstrap_servers=broker_ip,
        value_serializer=lambda v: str(v).encode('utf-8'),
        acks=0,
        linger_ms=0,
        request_timeout_ms=10000,
        max_block_ms=10000,
        api_version_auto_timeout_ms=10000,
    )

def read_data(file_path):
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            yield row

def run_producer(config):
    broker = config['kafka']['broker_ip']
    print(f"Connecting to Kafka broker at {broker} ...")
    producer = create_producer(broker)
    data_file = config['paths']['data_file']

    cpu_topic = config['kafka']['topics']['cpu']
    mem_topic = config['kafka']['topics']['mem']
    net_topic = config['kafka']['topics']['net']
    disk_topic = config['kafka']['topics']['disk']


    print(f"Streaming metrics from {data_file} ...")
    for row in read_data(data_file):
        try:
            # Force errors to surface quickly
            producer.send(cpu_topic, value=f"{row['ts']},{row['server_id']},{row['cpu_pct']}").get(timeout=5)
            producer.send(mem_topic, value=f"{row['ts']},{row['server_id']},{row['mem_pct']}").get(timeout=5)
            producer.send(net_topic, value=f"{row['ts']},{row['server_id']},{row['net_in']},{row['net_out']}").get(timeout=5)
            producer.send(disk_topic, value=f"{row['ts']},{row['server_id']},{row['disk_io']}").get(timeout=5)
            print(f"Sent data for {row['server_id']} at {row['ts']}")
        except KafkaError as e:
            print(f"ERROR sending to Kafka: {e}")
            break
        time.sleep(1)

    producer.flush()
    producer.close()

if __name__ == "__main__":
    with open('../config/config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    run_producer(config)
