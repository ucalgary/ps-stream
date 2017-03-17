import functools
import logging
import os
import sys

from confluent_kafka import Consumer, Producer
from docopt import docopt
from inspect import getdoc

from .. import collector
from .. import publisher
from .docopt_command import DocoptDispatcher
from .docopt_command import NoSuchCommand


log = logging.getLogger(__name__)


def main():
    command = dispatch()

    try:
        command()
    except KeyboardInterrupt:
        log.error('Aborting.')
        sys.exit(1)
    except:
        sys.exit(1)


def dispatch():
    dispatcher = DocoptDispatcher(
        PSStreamCommand,
        {'options_first': True})

    try:
        options, handler, command_options = dispatcher.parse(sys.argv[1:])
    except NoSuchCommand as e:
        commands = '\n'.join(parse_doc_section('commands:', getdoc(e.supercommand)))
        log.error('No such command: %s\n\n%s', e.command, commands)
        sys.exit(1)

    logging.basicConfig(level=logging.DEBUG if options['--verbose'] else logging.INFO)

    return functools.partial(perform_command, options, handler, command_options)


def perform_command(options, handler, command_options):
    command = PSStreamCommand()
    options = consolidated_options(options, command_options)
    handler(command, options)


class PSStreamCommand(object):
    """Process PeopleSoft sync messages into Kafka topics.

    Usage:
      ps-stream [--kafka=<arg>]... [--schema-registry=<arg>] [--zookeeper=<arg>]
                [--verbose]
                [COMMAND] [ARGS...]
      ps-stream -h|--help

    Options:
      -k, --kafka HOSTS             Kafka bootstrap hosts [default: kafka:9092]
      -r, --schema-registry URL     Avro schema registry url [default: http://schema-registry:80]
      --verbose                     Show more output

    Commands:
      collect            Collect PeopleSoft sync messages
      config             Validate and view the collector config
      publish            Parse transaction messages into record streams
    """

    def collect(self, options):
        """Collect PeopleSoft sync and fullsync messages.

        Usage: collect [--port=<arg>] [--target-prefix=<arg>] [--target-topic=<arg>]
                       [--accept-from=<arg>]...
                       [--accept-to=<arg>]...
                       [--accept-messagename=<arg>]...

        Options:
          --port PORT                 Port to listen to messages on [default: 8000]
          --accept-from NAMES         Accepted values for the From header
          --accept-to NAMES           Accepted values for the To header
          --accept-messagename NAMES  Accepted values for the MessageName header
          --target-prefix PREFIX      Prefix name for target topic [default: ps]
          --target-topic TOPIC        Topic to write transactions to [default: transactions]
        """
        config = kafka_config_from_options(options)
        producer = Producer(config)

        collector.collect(
          producer,
          topic=prefix_topics(options['--target-prefix'], (options['--target-topic'],)),
          port=int(options['--port']),
          senders=options['--accept-from'],
          recipients=options['--accept-to'],
          message_names=options['--accept-messagename'])

    def config(self, options):
        """Validate and view the collector config.

        Usage: config
        """
        pass

    def publish(self, options):
        """Parse transaction messages into record streams.

        Usage: publish [--source-prefix=<arg>] [--source-topic=<arg>]...
                       [--target-prefix=<arg>] [--target-topic=<arg>]
                       [options]

        Options:
          --source-prefix PREFIX     Prefix string for source topics [default: ps]
          --source-topic NAME        Topics to consume transactions from [default: transactions]
          --target-prefix PREFIX     Prefix name for target topics [default: ps]
          --target-topic NAME        Topic to write records to, defaults to the record type
          --consumer-group GROUP     Kafka consumer group name [default: ps-stream]
        """
        config = kafka_config_from_options(options)
        consumer = Consumer(config)
        producer = Producer(config)

        publisher.publish(
          consumer,
          producer,
          source_topics=prefix_topics(options['--source-prefix'], options['--source-topic']),
          target_topic=prefix_topics(options['--target-prefix'], options['--target-topic']))


def consolidated_options(options, command_options):
    environ_option_keys = ((k, 'PSSTREAM_' + k.lstrip('-').replace('-', '_').upper())
                           for k in (*options.keys(), *command_options.keys()))
    environ_options = {option_key: os.environ[environ_key]
                       for option_key, environ_key in environ_option_keys
                       if environ_key in os.environ}
    return {**environ_options, **options, **command_options}


def kafka_config_from_options(options):
    config = dict()

    if '--kafka' in options:
        config['bootstrap.servers'] = ','.join(options['--kafka'])
    if '--consumer-group' in options:
        config['group.id'] = options['--consumer-group']

    return config


def prefix_topics(prefix, topics):
    if not topics:
        return topics
    if prefix:
        if not isinstance(topics, str):
            return ['%s.%s'.format(prefix, topic) for topic in topics]
        else:
            return '%s.%s'.format(prefix, topics)
    return topics
