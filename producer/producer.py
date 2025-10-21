import yaml
import csv
import time
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError

def create_producer(broker_ip):
    """Creates a high-performance KafkaProducer."""
    print("Initializing high-performance Kafka producer...")
    return KafkaProducer(
        bootstrap_servers=broker_ip,
        value_serializer=lambda v: str(v).encode('utf-8'),
        # --- Advanced settings for high throughput ---
        acks='all',                           # Wait for all replicas to acknowledge.
        retries=5,                            # Retry up to 5 times on transient errors.
        max_in_flight_requests_per_connection=5, # Allow parallel requests.
        compression_type='snappy',            # Compress batches for network efficiency.
        batch_size=65536,                     # 64 KB batch size.
        linger_ms=10,                         # Wait up to 10ms to batch records.
        buffer_memory=67108864                # 64 MB buffer memory.
    )

def read_data(file_path):
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        # Yielding the reader directly to get a total count first
        rows = list(reader)
        yield len(rows)
        yield from rows

def run_producer(config):
    broker = config['kafka']['broker_ip']
    print(f"Connecting to Kafka broker at {broker} ...")
    producer = create_producer(broker)
    data_file = config['paths']['data_file']

    cpu_topic = config['kafka']['topics']['cpu']
    mem_topic = config['kafka']['topics']['mem']
    net_topic = config['kafka']['topics']['net']
    disk_topic = config['kafka']['topics']['disk']

    # --- Asynchronous send callbacks & statistics tracking ---
    success_count = {'cpu': 0, 'mem': 0, 'net': 0, 'disk': 0}
    error_count = {'cpu': 0, 'mem': 0, 'net': 0, 'disk': 0}

    def on_send_success(topic_name):
        def callback(metadata):
            nonlocal success_count
            success_count[topic_name] += 1
        return callback

    def on_send_error(topic_name):
        def callback(exc):
            nonlocal error_count
            print(f"❌ Failed to send to {topic_name}: {exc}")
            error_count[topic_name] += 1
        return callback

    print(f"Streaming metrics from {data_file} ...")
    data_generator = read_data(data_file)
    total_records = next(data_generator)

    start_time = time.time()
    for index, row in enumerate(data_generator):
        try:
            # Format messages as comma-separated strings for consumer compatibility
            cpu_val = f"{row['ts']},{row['server_id']},{row['cpu_pct']}"
            mem_val = f"{row['ts']},{row['server_id']},{row['mem_pct']}"
            net_val = f"{row['ts']},{row['server_id']},{row['net_in']},{row['net_out']}"
            disk_val = f"{row['ts']},{row['server_id']},{row['disk_io']}"

            # Asynchronously send with callbacks, no blocking .get()
            producer.send(cpu_topic, value=cpu_val).add_callback(on_send_success('cpu')).add_errback(on_send_error('cpu'))
            producer.send(mem_topic, value=mem_val).add_callback(on_send_success('mem')).add_errback(on_send_error('mem'))
            producer.send(net_topic, value=net_val).add_callback(on_send_success('net')).add_errback(on_send_error('net'))
            producer.send(disk_topic, value=disk_val).add_callback(on_send_success('disk')).add_errback(on_send_error('disk'))

            if (index + 1) % 1000 == 0:
                 print(f"  Processed {index + 1}/{total_records} records...")

        except Exception as e:
            print(f"❌ Error preparing record {index + 1}: {e}")

    print("\n🔄 Flushing all outstanding messages... (this may take a moment)")
    producer.flush()
    print("✅ Flush complete.")
    producer.close()

    end_time = time.time()
    duration = end_time - start_time

    print("\n" + "="*60)
    print("📊 FINAL PRODUCER STATISTICS:")
    print(f"Total records processed: {total_records}")
    print(f"Total time: {duration:.2f} seconds")
    if duration > 0:
        print(f"Average throughput: {total_records / duration:.2f} records/sec")
    print("-" * 20)
    print(f"topic-cpu:  {success_count['cpu']} ✓ | {error_count['cpu']} ✗")
    print(f"topic-mem:  {success_count['mem']} ✓ | {error_count['mem']} ✗")
    print(f"topic-net:  {success_count['net']} ✓ | {error_count['net']} ✗")
    print(f"topic-disk: {success_count['disk']} ✓ | {error_count['disk']} ✗")
    print("="*60)


if __name__ == "__main__":
    with open('../config/config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    run_producer(config)
