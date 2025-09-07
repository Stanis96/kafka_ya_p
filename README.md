# Инструкция по работе с Kafka-кластером в Docker

## Используемые параметры конфигурации:

### KAFKA-KRAFT

| Параметр                             | Значение                                        | Описание                                                                                                                      |
|------------------------------------|------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|
| `KAFKA_SCHEMA_REGISTRY_URL`        | `schema-registry:8081`                          | URL сервиса Schema Registry для управления схемами сериализации сообщений Kafka.                                              |
| `KAFKA_ENABLE_KRAFT`               | `yes`                                          | Включение режима KRaft (Kafka Raft Metadata mode) — режима работы Kafka без ZooKeeper.                                        |
| `KAFKA_CFG_ALLOW_PLAINTEXT_LISTENER` | `yes`                                       | Разрешает прослушивание подключений без шифрования (plaintext).                                                               |
| `KAFKA_CFG_PROCESS_ROLES`          | `broker,controller`                             | Роли, которые выполняет данный узел: брокер и контроллер.                                                                      |
| `KAFKA_KRAFT_CLUSTER_ID`           | `abcdefghijklmnopqrstuv`                        | Уникальный идентификатор кластера KRaft.                                                                                      |
| `KAFKA_CFG_LISTENERS`              | `PLAINTEXT://0.0.0.0:9092,EXTERNAL://0.0.0.0:9094,CONTROLLER://0.0.0.0:9093` | Адреса и порты, на которых Kafka слушает входящие соединения по разным протоколам (PLAINTEXT — основной брокер, EXTERNAL — внешний, CONTROLLER — внутренний контроллер). |
| `KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP` | `CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT` | Соответствие между именами слушателей и используемыми протоколами безопасности.                                               |
| `KAFKA_CFG_NODE_ID`                | `0`                                            | Уникальный идентификатор узла Kafka в кластере.                                                                               |
| `KAFKA_CFG_CONTROLLER_QUORUM_VOTERS` | `0@kafka-0:9093,1@kafka-1:9093`              | Список контроллеров в кворуме с указанием node_id и адресов для взаимодействия контроллеров.                                   |
| `KAFKA_CFG_ADVERTISED_LISTENERS`   | `PLAINTEXT://kafka-0:9092,EXTERNAL://localhost:9094,CONTROLLER://kafka-0:9093` | Адреса, которые Kafka рекламирует клиентам для подключения (могут отличаться от внутренних адресов).                          |

### Дополнительно по UI (Kafka UI)

| Параметр                             | Значение       | Описание                                                                                      |
|--------------------------------------|----------------|-----------------------------------------------------------------------------------------------|
| `KAFKA_CLUSTERS_0_BOOTSTRAP_SERVERS` | `kafka-n:9092` | Адрес Kafka брокера, который UI будет использовать для подключения (обычно `<hostname>:<port>`). |
| `KAFKA_CLUSTERS_0_NAME`              | `kraft`        | Имя кластера для отображения в UI.                                                            |

## Как проверить работу Kafka через Kafka UI

Откройте веб-браузер и перейдите по адресу:
http://localhost:8080

# Проверка практической работы #2

## 1. Запуск проекта:

### 1. Откройте терминал в корне проекта
### 2. Запустите командой:
```bash
   docker compose -f src/practice_2/docker-compose.yaml up -d
   ```

## 2. Структура проекта:

### 1. scr/practice_2/base.py
- Базовые конфигурации для Kafka

### 2. src/practice_2/producer.py
- Класс продьюсер `KafkaOrderProducer`
- Запуск продьюсера
- В `docker-compose.yaml` для сервиса продьюсера `kafka-producer` задаются параметры:
  - `SEND_MSG_INTERVAL_SECONDS` - интервал между отправками сообщений
  - `SEND_MSG_COUNT` - количество отправленных сообщений
>Tip: Также предусмотрено 1 сообщение для вывода с ошибкой сериализации

### 3. src/practice_2/consumer.py
- Класс консьюмер `KafkaOrderConsumer`

### 4. src/practice_2/run_consumers.py
- Запуск консьюмеров
- В `docker-compose.yaml` для сервисов консьюмеров `batch-kafka-consumer` & `single-kafka-consumer` задается параметр:
  - `BATCH_SIZE` - размер батча, исходя из которого будет выбран тип консьюмера

### 5. src/practice_2/topic.txt
- Используемые команды для создания топика и проверки его состояния

## 3. Просмотр результата

### 1. Проверка отправки сообщений продьюсером `kafka-producer`:
   ```bash
      docker logs -f kafka-producer
  ```

### 2. Проверка получения сообщений консьюмерами `batch-kafka-consumer` & `single-kafka-consumer`:
   ```bash
      docker logs -f batch-kafka-consumer
      docker logs -f single-kafka-consumer
   ```

# Проверка практической работы #3

## 1. Запуск проекта:

### 1. Откройте терминал в корне проекта
### 2. Запустите командой:
```bash
   docker compose -f src/practice_3/docker-compose.yaml up -d
   ```
## 2. Структура проекта:

### 1. scr/practice_3/base.py
- Базовые конфигурации

### 2. scr/practice_3/models.py
- Модели для потоков сообщений

### 3. scr/practice_3/run_app.py
- инициализация приложения и сущностей faust;
- обработчики потоков;
- задача для отправки тестовых сообщений;
- запуск приложения faust

## 3. Просмотр результата

### 1. Проверка логов приложения faust `faust-app`:
   ```bash
      docker logs -f faust-app
  ```

### 2. Ручная отправка тестовых данных:

- Добавление/удаление запрещенного слова в topic `forbidden_words`:
   ```json
      {
      "action": "add", # "remove"
      "word": "плохоеслово"
  }
  ```
  
- Блокировка пользователя в topic `blocked_users`:
   ```json
      {
      "user": "Игорь",
      "blocked": "Виктор"
  }
  ```
  
- Отправка сообщений в topic `messages`:
   ```json
      {
      "sender": "Игорь",
      "recipient": "Андрей",
      "text": "Вот и плохоеслово"
  }
  ```
  
   ```json
      {
      "sender": "Виктор",
      "recipient": "Игорь",
      "text": "Не дойдет"
  }
  ```
  
- В topic `filtered_messages` можно наблюдать соответствующий результат.

# Проверка практической работы #4

## 1. Запуск проекта:

### 1. Откройте терминал в корне проекта
### 2. Запустите командой:
```bash
   docker compose -f src/practice_4/docker-compose.yaml up -d
   ```

## 2. Проверка задания:

### 1. Топик `balanced_topic` создается с 8 партициями и 3 репликами в `kafka-init` контейнере

### 2. Попадаем внутрь контейнера `kafka-0`
```bash
   docker exec -it kafka-0 bin/bash
   ```

### 3. Проверка текущего распределения партиций
```bash
   kafka-topics.sh --bootstrap-server localhost:9092 --topic balanced_topic --describe
   ```

### 4. Создаем json-файл `rebalance_topic.json`
```bash
  cat > /tmp/rebalance_topic.json <<EOF
  {
    "version": 1,
    "topics": [
      {
        "topic": "balanced_topic"
      }
    ]
  }
  EOF
   ```

### 5. Генерация нового распределения
```bash
   kafka-reassign-partitions.sh --bootstrap-server localhost:9092 --broker-list "0,1,2" --topics-to-move-json-file "/tmp/rebalance_topic.json" --generate
   ```

### 6. Результат записываем в файл `reassignment.json`
```bash
  cat > /tmp/reassignment.json <<EOF
  {"version":1,"partitions":[
    {"topic":"balanced_topic","partition":0,"replicas":[0,2,1],"log_dirs":["any","any","any"]},
    {"topic":"balanced_topic","partition":1,"replicas":[1,0,2],"log_dirs":["any","any","any"]},
    {"topic":"balanced_topic","partition":2,"replicas":[2,1,0],"log_dirs":["any","any","any"]},
    {"topic":"balanced_topic","partition":3,"replicas":[0,1,2],"log_dirs":["any","any","any"]},
    {"topic":"balanced_topic","partition":4,"replicas":[1,2,0],"log_dirs":["any","any","any"]},
    {"topic":"balanced_topic","partition":5,"replicas":[2,0,1],"log_dirs":["any","any","any"]},
    {"topic":"balanced_topic","partition":6,"replicas":[0,2,1],"log_dirs":["any","any","any"]},
    {"topic":"balanced_topic","partition":7,"replicas":[1,0,2],"log_dirs":["any","any","any"]}
  ]}
  EOF
   ```
### 7. Перераспределяем

```bash
   kafka-reassign-partitions.sh --bootstrap-server localhost:9092 --reassignment-json-file /tmp/reassignment.json --execute
   ```

### 8. Проверка перераспределения

```bash
   kafka-reassign-partitions.sh --bootstrap-server localhost:9092 --reassignment-json-file /tmp/reassignment.json --verify
   ```

### 9. Моделируем сбой
```bash
   docker stop kafka-1
   ```

### 10. Проверка текущего состояния (в поле `Isr` на 1 реплику меньше)
```bash
   kafka-topics.sh --bootstrap-server localhost:9092 --topic balanced_topic --describe
   ```

### 11. Восстанавливаем
```bash
   docker start kafka-1
   ```

### 12. Проверка текущего состояния (в поле `Isr` реплик снова 3)
```bash
   kafka-topics.sh --bootstrap-server localhost:9092 --topic balanced_topic --describe
   ```
