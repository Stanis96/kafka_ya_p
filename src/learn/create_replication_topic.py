from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic


admin = AdminClient({"bootstrap.servers": "localhost:29092"})
# Создаем новый топик с указанными параметрами
new_topic = NewTopic(
    "my-topic3",  # Имя топика
    num_partitions=1,  # Количество партиций
    replication_factor=1,  # Фактор репликации
    config={"min.insync.replicas": "2"},  # Минимум 2 реплики должны подтвердить запись
)
admin.create_topics([new_topic])


# Создаем KafkaProducer
conf = {
    "bootstrap.servers": "localhost:29092",
    "acks": "all",  # Для синхронной репликации
    "retries": 3,  # Количество попыток при сбоях
}
producer = Producer(conf)

producer.produce(
    "my-topic3",
    key="key",
    value="value",
)
producer.flush()
