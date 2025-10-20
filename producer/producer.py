import yaml
import csv
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError

def create_producer(broker_ip, fast_mode=False):
    # Short timeouts so we fail fast if broker is unreachable
    if fast_mode:
        # Maximum speed: batch messages, no waiting
        return KafkaProducer(
            bootstrap_servers=broker_ip,
            value_serializer=lambda v: str(v).encode('utf-8'),
            acks=0,  # Don't wait for broker acknowledgment
            linger_ms=10,  # Wait 10ms to batch messages
            batch_size=32768,  # 32KB batches
            buffer_memory=67108864,  # 64MB buffer
            compression_type='snappy',  # Compress for faster network transfer
            request_timeout_ms=30000,
            max_block_ms=10000,
            api_version_auto_timeout_ms=10000,
        )
    else:
        # Normal mode with verification
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

def run_producer(config, fast_mode=False):
    broker = config['kafka']['broker_ip']
    print(f"Connecting to Kafka broker at {broker} ...")
    producer = create_producer(broker, fast_mode)
    data_file = config['paths']['data_file']

    if fast_mode:
        print("🚀 FAST MODE ENABLED: Sending messages at maximum speed...")

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
    message_count = 0
    start_time = time.time()

    for row in read_data(data_file):
        try:
            if fast_mode:
                # Asynchronous sending - fire and forget
                producer.send(cpu_topic, value=f"{row['ts']},{row['server_id']},{row['cpu_pct']}")
                producer.send(mem_topic, value=f"{row['ts']},{row['server_id']},{row['mem_pct']}")
                producer.send(net_topic, value=f"{row['ts']},{row['server_id']},{row['net_in']},{row['net_out']}")
                producer.send(disk_topic, value=f"{row['ts']},{row['server_id']},{row['disk_io']}")
                message_count += 1
                # Print progress every 1000 messages
                if message_count % 1000 == 0:
                    elapsed = time.time() - start_time
                    rate = message_count / elapsed if elapsed > 0 else 0
                    print(f"Sent {message_count} records ({rate:.0f} records/sec)...")
            else:
                # Synchronous sending with confirmation
                producer.send(cpu_topic, value=f"{row['ts']},{row['server_id']},{row['cpu_pct']}").get(timeout=5)
                producer.send(mem_topic, value=f"{row['ts']},{row['server_id']},{row['mem_pct']}").get(timeout=5)
                producer.send(net_topic, value=f"{row['ts']},{row['server_id']},{row['net_in']},{row['net_out']}").get(timeout=5)
                producer.send(disk_topic, value=f"{row['ts']},{row['server_id']},{row['disk_io']}").get(timeout=5)
                print(f"Sent data for {row['server_id']} at {row['ts']}")
                time.sleep(0.1)  # 0.1 seconds delay in normal mode
        except KafkaError as e:
            print(f"ERROR sending to Kafka: {e}")
            break

    elapsed = time.time() - start_time
    if fast_mode:
        print(f"\n✅ Completed! Sent {message_count} records in {elapsed:.2f} seconds")
        print(f"   Average rate: {message_count/elapsed:.0f} records/sec")

    producer.flush()
    producer.close()

if __name__ == "__main__":
    with open('../config/config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    run_producer(config)
