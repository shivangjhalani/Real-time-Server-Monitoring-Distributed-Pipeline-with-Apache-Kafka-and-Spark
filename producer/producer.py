import yaml
import csv
import time
from kafka import KafkaProducer

def create_producer(broker_ip):
    return KafkaProducer(bootstrap_servers=broker_ip,
                         value_serializer=lambda v: str(v).encode('utf-8'))

def read_data(file_path):
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            yield row

def run_producer(config):
    producer = create_producer(config['kafka']['broker_ip'])
    data_file = config['paths']['data_file']

    cpu_topic = config['kafka']['topics']['cpu']
    mem_topic = config['kafka']['topics']['mem']
    net_topic = config['kafka']['topics']['net']
    disk_topic = config['kafka']['topics']['disk']

    for row in read_data(data_file):
        producer.send(cpu_topic, value=f"{row['ts']},{row['server_id']},{row['cpu_pct']}")
        producer.send(mem_topic, value=f"{row['ts']},{row['server_id']},{row['mem_pct']}")
        producer.send(net_topic, value=f"{row['ts']},{row['server_id']},{row['net_in']},{row['net_out']}")
        producer.send(disk_topic, value=f"{row['ts']},{row['server_id']},{row['disk_io']}")
        print(f"Sent data for {row['server_id']} at {row['ts']}")
        time.sleep(1)

    producer.flush()
    producer.close()

if __name__ == "__main__":
    with open('../config/config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    run_producer(config)
