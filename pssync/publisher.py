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

                for topic, key, value in messages_from_transaction(transaction):
                    print(f'{topic} {key}')
                    # self.producer.produce(topic, value, key)
            elif message.error().code() != KafkaError._PARTITION_EOF:
                print(message.error())
                self.running = False

        print('closing consumer')
        consumer.close()

    def messages_from_transaction(self, transaction):
        yield None, None, None


def publish(consumer, producer, source_topics=None, destination_topic=None):
    publisher = PSSyncPublisher(consumer, producer, source_topics=None, destination_topic=None)
    print(f'Reading transactions from {source_topics}')
    publisher.run()
