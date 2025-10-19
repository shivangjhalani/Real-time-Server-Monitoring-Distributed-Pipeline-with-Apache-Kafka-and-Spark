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

def run_consumer1(config):
    broker_ip = config['kafka']['broker_ip']
    cpu_topic = config['kafka']['topics']['cpu']
    mem_topic = config['kafka']['topics']['mem']
    output_dir = config['paths']['output_dir']

    consumer = create_consumer(broker_ip, [cpu_topic, mem_topic])

    cpu_file_path = os.path.join(output_dir, 'cpu_data.csv')
    mem_file_path = os.path.join(output_dir, 'mem_data.csv')

    with open(cpu_file_path, 'w') as cpu_file, open(mem_file_path, 'w') as mem_file:
        cpu_file.write("ts,server_id,cpu_pct\n")
        mem_file.write("ts,server_id,mem_pct\n")

        for message in consumer:
            if message.topic == cpu_topic:
                cpu_file.write(message.value + '\n')
                cpu_file.flush()
                print(f"CPU data received: {message.value}")
            elif message.topic == mem_topic:
                mem_file.write(message.value + '\n')
                mem_file.flush()
                print(f"Memory data received: {message.value}")

if __name__ == "__main__":
    with open('../config/config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    if not os.path.exists(config['paths']['output_dir']):
        os.makedirs(config['paths']['output_dir'])
    run_consumer1(config)
