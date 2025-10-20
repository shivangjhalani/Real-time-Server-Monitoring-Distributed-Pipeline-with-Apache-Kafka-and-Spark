import yaml
import os
from kafka import KafkaConsumer

def create_consumer(broker_ip, topics, group_id):
    return KafkaConsumer(
        *topics,
        bootstrap_servers=broker_ip,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=group_id,
        value_deserializer=lambda x: x.decode('utf-8'),
        api_version_auto_timeout_ms=10000,
        request_timeout_ms=30000
    )

def run_consumer2(config):
    broker_ip = config['kafka']['broker_ip']
    net_topic = config['kafka']['topics']['net']
    disk_topic = config['kafka']['topics']['disk']
    output_dir = config['paths']['output_dir']

    print(f"Consumer2 connecting to Kafka broker at {broker_ip} ...")
    print(f"Subscribing to topics: {net_topic}, {disk_topic}")

    consumer = create_consumer(broker_ip, [net_topic, disk_topic], 'consumer2-group')

    print(f"Consumer connected. Waiting for messages...")

    net_file_path = os.path.join(output_dir, 'net_data.csv')
    disk_file_path = os.path.join(output_dir, 'disk_data.csv')

    message_count = 0
    with open(net_file_path, 'w') as net_file, open(disk_file_path, 'w') as disk_file:
        net_file.write("ts,server_id,net_in,net_out\n")
        disk_file.write("ts,server_id,disk_io\n")

        print("Starting to consume messages...")
        try:
            for message in consumer:
                message_count += 1
                if message.topic == net_topic:
                    net_file.write(message.value + '\n')
                    net_file.flush()
                    print(f"[{message_count}] Network data received: {message.value}")
                elif message.topic == disk_topic:
                    disk_file.write(message.value + '\n')
                    disk_file.flush()
                    print(f"[{message_count}] Disk data received: {message.value}")
        except KeyboardInterrupt:
            print(f"\nConsumer stopped. Total messages received: {message_count}")
        finally:
            consumer.close()

if __name__ == "__main__":
    with open('../config/config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    if not os.path.exists(config['paths']['output_dir']):
        os.makedirs(config['paths']['output_dir'])
    run_consumer2(config)
