import logging
import sys

from .. import collector
from .. import publisher

from docopt_utils.dispatcher import dispatch


log = logging.getLogger(__name__)


def main():
    def set_logging_level(handler, options):
        logging.basicConfig(level=logging.DEBUG if options['--verbose'] else logging.INFO)

    command_classes = {'__root__': PSStreamCommand}
    dispatch(command_classes, env='PSSTREAM', before_f=set_logging_level)


class PSStreamCommand(object):
    """Process PeopleSoft sync messages into Kafka topics.

    Usage:
      ps-stream [--kafka=<arg>]...
                [--verbose]
                [COMMAND] [ARGS...]
      ps-stream -h|--help

    Options:
      -k, --kafka HOSTS             Kafka bootstrap hosts [default: kafka:9092]
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

        collector.collect(
          config,
          topic=prefix_topics(options['--target-prefix'], options['--target-topic']),
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

        publisher.publish(
          config,
          source_topics=prefix_topics(options['--source-prefix'], options['--source-topic']),
          target_topic=prefix_topics(options['--target-prefix'], options['--target-topic']),
          target_prefix=options['--target-prefix'])


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
            return [f'{prefix}.{topic}' for topic in topics]
        else:
            return f'{prefix}.{topics}'
    return topics
