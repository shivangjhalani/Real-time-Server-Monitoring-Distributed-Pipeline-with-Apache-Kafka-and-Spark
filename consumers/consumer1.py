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

def run_consumer1(config):
    broker_ip = config['kafka']['broker_ip']
    cpu_topic = config['kafka']['topics']['cpu']
    mem_topic = config['kafka']['topics']['mem']
    output_dir = config['paths']['output_dir']

    print(f"Consumer1 connecting to Kafka broker at {broker_ip} ...")
    print(f"Subscribing to topics: {cpu_topic}, {mem_topic}")

    consumer = create_consumer(broker_ip, [cpu_topic, mem_topic], 'consumer1-group')

    print(f"Consumer connected. Waiting for messages...")
    print(f"Partitions assigned: {consumer.assignment()}")

    cpu_file_path = os.path.join(output_dir, 'cpu_data.csv')
    mem_file_path = os.path.join(output_dir, 'mem_data.csv')

    message_count = 0
    with open(cpu_file_path, 'w') as cpu_file, open(mem_file_path, 'w') as mem_file:
        cpu_file.write("ts,server_id,cpu_pct\n")
        mem_file.write("ts,server_id,mem_pct\n")

        print("Starting to consume messages...")
        try:
            for message in consumer:
                message_count += 1
                if message.topic == cpu_topic:
                    cpu_file.write(message.value + '\n')
                    cpu_file.flush()
                    print(f"[{message_count}] CPU data received: {message.value}")
                elif message.topic == mem_topic:
                    mem_file.write(message.value + '\n')
                    mem_file.flush()
                    print(f"[{message_count}] Memory data received: {message.value}")
        except KeyboardInterrupt:
            print(f"\nConsumer stopped. Total messages received: {message_count}")
        finally:
            consumer.close()

if __name__ == "__main__":
    with open('../config/config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    if not os.path.exists(config['paths']['output_dir']):
        os.makedirs(config['paths']['output_dir'])
    run_consumer1(config)
