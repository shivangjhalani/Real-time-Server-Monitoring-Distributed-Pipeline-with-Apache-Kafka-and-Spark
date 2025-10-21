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
        acks='all',
        linger_ms=20,
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

    # Force metadata fetch for topics to fail fast if broker/topics unreachable
    for t in [cpu_topic, mem_topic, net_topic, disk_topic]:
        parts = producer.partitions_for(t)
        if parts is None:
            print(f"Waiting for topic metadata: {t} ...")
            # Give a brief grace period
            for _ in range(5):
                time.sleep(1)
                parts = producer.partitions_for(t)
                if parts is not None:
                    break
        if parts is None:
            print(f"ERROR: Could not fetch metadata for topic '{t}'.\n"
                  f"- Verify broker '{broker}' is reachable from this machine.\n"
                  f"- Ensure advertised.listeners on broker uses the ZeroTier IP.\n"
                  f"- Ensure topic exists or auto.create.topics.enable=true on broker.")
            try:
                producer.close()
            except Exception:
                pass
            return

    print(f"Streaming metrics from {data_file} ...")
    for row in read_data(data_file):
        try:
            # Force errors to surface quickly
            producer.send(cpu_topic, value=f"{row['ts']},{row['server_id']},{row['cpu_pct']}").get(timeout=30)
            producer.send(mem_topic, value=f"{row['ts']},{row['server_id']},{row['mem_pct']}").get(timeout=30)
            producer.send(net_topic, value=f"{row['ts']},{row['server_id']},{row['net_in']},{row['net_out']}").get(timeout=30)
            producer.send(disk_topic, value=f"{row['ts']},{row['server_id']},{row['disk_io']}").get(timeout=30)
            print(f"Sent data for {row['server_id']} at {row['ts']}")
        except KafkaError as e:
            print(f"ERROR sending to Kafka: {e}")
            break
        time.sleep(0.01)

    producer.flush()
    producer.close()

if __name__ == "__main__":
    with open('../config/config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    run_producer(config)
