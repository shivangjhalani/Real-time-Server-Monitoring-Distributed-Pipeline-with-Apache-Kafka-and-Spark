import yaml
import os
from kafka import KafkaConsumer

def create_consumer(broker_ip, topics):
    return KafkaConsumer(
        *topics,
        bootstrap_servers=broker_ip,
        auto_offset_reset='earliest',
        value_deserializer=lambda x: x.decode('utf-8')
    )

def run_consumer2(config):
    broker_ip = config['kafka']['broker_ip']
    net_topic = config['kafka']['topics']['net']
    disk_topic = config['kafka']['topics']['disk']
    output_dir = config['paths']['output_dir']

    consumer = create_consumer(broker_ip, [net_topic, disk_topic])

    net_file_path = os.path.join(output_dir, 'net_data.csv')
    disk_file_path = os.path.join(output_dir, 'disk_data.csv')

    with open(net_file_path, 'w') as net_file, open(disk_file_path, 'w') as disk_file:
        net_file.write("ts,server_id,net_in,net_out\n")
        disk_file.write("ts,server_id,disk_io\n")

        for message in consumer:
            if message.topic == net_topic:
                net_file.write(message.value + '\n')
                net_file.flush()
                print(f"Network data received: {message.value}")
            elif message.topic == disk_topic:
                disk_file.write(message.value + '\n')
                disk_file.flush()
                print(f"Disk data received: {message.value}")

if __name__ == "__main__":
    with open('../config/config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    if not os.path.exists(config['paths']['output_dir']):
        os.makedirs(config['paths']['output_dir'])
    run_consumer2(config)
