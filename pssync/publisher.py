import logging
import pkg_resources
from difflib import SequenceMatcher
from xml.etree import ElementTree

import ujson as json
import yaml
from confluent_kafka import KafkaError

from .utils import element_to_obj


log = logging.getLogger(__name__)


key_attributes_by_record_name = yaml.load(
    pkg_resources.resource_stream(__name__, 'publisher.yml'))['record keys']


class PSSyncPublisher(object):

    def __init__(self, consumer, producer, source_topics=None, destination_topic=None):
        super().__init__()
        self.consumer = consumer
        self.producer = producer
        self.source_topics = source_topics
        self.destination_topic = destination_topic
        self.running = True

    def run(self):
        '''Process transactions from the source topics and publish
        messages representing a stream of PeopleSoft rows organized
        by record name.
        '''
        self.consumer.subscribe(self.source_topics)

        while self.running:
            message = self.consumer.poll(timeout=30)

            if not message:
                continue
            elif not message.error():
                transaction = json.loads(message.value().decode('utf-8'))

                for topic, key, value in self.messages_from_transaction(transaction):
                    self.producer.produce(topic, value, key)
            elif message.error().code() != KafkaError._PARTITION_EOF:
                print(message.error())
                self.running = False

        self.terminate()

    def terminate(self):
        log.info('Terminating')
        self.consumer.close()
        self.producer.flush()

    def messages_from_transaction(self, transaction, key_serde=json.dumps, value_serde=json.dumps):
        transaction['Transaction'] = element_to_obj(
            ElementTree.fromstring(transaction['Transaction']), wrap_value=False)

        audit_actn = transaction['Transaction']['PSCAMA']['AUDIT_ACTN']
        if audit_actn not in ('A', 'C', 'D'):
            log.info('Empty AUDIT_ACTN received')
            log.debug(transaction)
            return

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

    def key_for_record(self, record_type, record_data, guess=False):
        key_attribute = key_attributes_by_record_name.get(record_type, None)
        if not key_attribute and guess:
            keys = record_data.keys()
            keys = sorted(keys,
                          key=lambda x: SequenceMatcher(a=record_type, b=x).ratio(),
                          reverse=True)
            key_attribute = keys[0]
            key_attributes_by_record_name[record_type] = key_attribute
        return key_attribute and record_data.get(key_attribute, None)


def publish(consumer, producer, source_topics=None, destination_topic=None):
    publisher = PSSyncPublisher(
        consumer, producer,
        source_topics=source_topics, destination_topic=destination_topic)
    log.info(f'Reading transactions from {source_topics}')
    publisher.run()
