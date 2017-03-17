import logging
import pkg_resources
import signal
import sys
from difflib import SequenceMatcher
from xml.etree import ElementTree

import ujson as json
import yaml
from confluent_kafka import Consumer, Producer
from confluent_kafka import KafkaError

from .utils import element_to_obj


log = logging.getLogger(__name__)


key_formats_by_record_type = yaml.load(
    pkg_resources.resource_stream(__name__, 'publisher.yml'))['message_keys']


class PSStreamPublisher(object):

    def __init__(self, consumer, producer,
                 source_topics=None, target_topic=None, target_prefix=None):
        super().__init__()
        self.consumer = consumer
        self.producer = producer
        self.source_topics = source_topics
        self.target_topic = target_topic
        self.target_prefix = target_prefix
        self.running = True

    def run(self):
        '''Process transactions from the source topics and publish
        messages representing a stream of PeopleSoft rows organized
        by record name.
        '''
        signal.signal(signal.SIGINT, self.terminate)
        signal.signal(signal.SIGTERM, self.terminate)

        self.consumer.subscribe(self.source_topics)

        while self.running:
            message = self.consumer.poll(timeout=5)

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
        sys.exit(0)

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
            log.debug(f'Producing to topic {topic} with key {key}')
            if key and key_serde:
                key = key_serde(key)
            if value and value_serde:
                value = value_serde(value)
            yield topic, key, value

    def topic_for_record(self, record_type, record_data):
        if self.target_topic:
            return self.target_topic
        elif self.target_prefix:
            return f'{self.target_prefix}.{record_type}'
        return record_type

    def key_for_record(self, record_type, record_data, guess=False):
        key_format = key_formats_by_record_type.get(record_type, None)
        if not key_format and guess:
            keys = record_data.keys()
            keys = sorted(keys,
                          key=lambda x: SequenceMatcher(a=record_type, b=x).ratio(),
                          reverse=True)
            key_attribute = keys[0]
            key_format = '{%s}' % key_attribute
            key_formats_by_record_type[record_type] = key_format
        return key_format and key_format.format(**record_data)


def publish(config, source_topics=None, target_topic=None, target_prefix=None):
    consumer_config = {**config, ** {
        'default.topic.config': {
            'auto.offset.reset': 'smallest',
            'auto.commit.interval.ms': 5000
        }
    }}
    producer_config = config
    consumer = Consumer(consumer_config)
    producer = Producer(producer_config)
    publisher = PSStreamPublisher(
        consumer, producer,
        source_topics=source_topics, target_topic=target_topic, target_prefix=target_prefix)
    log.info(f'Reading transactions from {source_topics}')
    publisher.run()
