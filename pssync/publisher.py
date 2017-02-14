import json

from confluent_kafka import KafkaError


class PSSyncPublisher(object):

    def __init__(self, consumer, producer, source_topics=None, destination_topic=None):
        super().__init__()
        self.consumer = consumer
        self.producer = producer
        self.source_topics = source_topics
        self.destination_topic = destination_topic
        self.running = True

    def run(self):
        self.consumer.subscribe(self.source_topics)

        while self.running:
            message = self.consumer.poll(timeout=30)

            if not message:
                self.running = False
            elif not message.error():
                transaction = json.loads(message.value().decode('utf-8'))

                for topic, key, value in self.messages_from_transaction(transaction):
                    print(f'{topic} {key}')
                    # self.producer.produce(topic, value, key)
            elif message.error().code() != KafkaError._PARTITION_EOF:
                print(message.error())
                self.running = False

        print('closing consumer')
        self.consumer.close()

    def messages_from_transaction(self, transaction, key_serde=json.dumps, value_serde=json.dumps):
        audit_actn = transaction['Transaction']['PSCAMA']['AUDIT_ACTN']
        assert(audit_actn in ('A', 'C', 'D'))

        for record_type, record_data in transaction['Transaction'].items():
            if record_type == 'PSCAMA':
                continue
            topic = self.topic_for_record(record_type, record_data)
            key = self.key_for_record(record_type, record_data)
            value = audit_actn in ('A', 'C') and record_data or None
            if key and key_serde:
                key = key_serde(key)
            if value and value_serde:
                value = value_serde(value)
            yield topic, key, value

    def topic_for_record(self, record_type, record_data):
        return self.destination_topic or record_type

    def key_for_record(self, record_type, record_data, default=None):
        return default


def publish(consumer, producer, source_topics=None, destination_topic=None):
    publisher = PSSyncPublisher(consumer, producer, source_topics=source_topics, destination_topic=destination_topic)
    print(f'Reading transactions from {source_topics}')
    publisher.run()
