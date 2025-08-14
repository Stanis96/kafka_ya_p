from confluent_kafka.admin import AdminClient, NewTopic

admin = AdminClient({"bootstrap.servers": "127.0.0.1:9094"})

new_topic = NewTopic(
    "my-topic4",
    num_partitions=1,
    replication_factor=1,
)

fs = admin.create_topics([new_topic])

for topic, f in fs.items():
    try:
        f.result()
        print(f"Topic '{topic}' created successfully.")
    except Exception as e:
        print(f"Failed to create topic '{topic}': {e}")
