import functools
import logging
import sys

from confluent_kafka import Consumer, Producer
from docopt import docopt
from inspect import getdoc

from .. import collector
from .docopt_command import DocoptDispatcher
from .docopt_command import NoSuchCommand


log = logging.getLogger(__name__)


def main():
    command = dispatch()

    try:
        command()
    except (KeyboardInterrupt, signals.ShutdownException):
        log.error('Aborting.')
        sys.exit(1)
    except:
        sys.exit(1)


def dispatch():
    dispatcher = DocoptDispatcher(
        PSSyncCommand,
        {'options_first': True})

    try:
        options, handler, command_options = dispatcher.parse(sys.argv[1:])
    except NoSuchCommand as e:
        commands = '\n'.join(parse_doc_section('commands:', getdoc(e.supercommand)))
        log.error('No such command: %s\n\n%s', e.command, commands)
        sys.exit(1)

    return functools.partial(perform_command, options, handler, command_options)


def perform_command(options, handler, command_options):
    command = PSSyncCommand()
    handler(command, options, command_options)


class PSSyncCommand(object):
    """Process PeopleSoft sync messages into Kafka topics.

    Usage:
      pssync [--kafka=<arg>]... [--schema-registry=<arg>]
             [--zookeeper=<arg>] [--topic-prefix=<arg>]
             [COMMAND] [ARGS...]
      pssync -h|--help

    Options:
      -k, --kafka HOSTS             Kafka brokers [default: kafka:9092]
      -r, --schema-registry URL     Avro schema registry host [default: schema-registry:80]
      -z, --zookeeper HOST          Zookeeper host [default: zookeeper:2181]
      -p, --topic-prefix PREFIX     String to prepend to all topic names

    Commands:
      collect            Collect PeopleSoft sync messages
      config             Validate and view the collector config
      parse              Parse sync message streams into record streams
    """

    def collect(self, options, command_options):
        """Collect PeopleSoft sync and fullsync messages.

        Usage: collect [--port=<arg>] [--topic=<arg>]
                       [--senders=<arg>]...
                       [--recipients=<arg>]...
                       [--messages=<arg>]...

        Options:
          --port PORT           Port to listen to messages on [default: 8000]
          --senders NAMES       Accepted values for the From header
          --recipients NAMES    Accepted values for the To header
          --messages NAMES      Accepted values for the MessageName header
          --topic TOPIC         Produce to a specific Kafka topic, otherwise
                                messages are sent to topics by message name
        """
        bootstraps = kafka_bootstraps_from_options(options)
        producer = Producer({'bootstrap.servers': bootstraps})

        collector.collect(
          producer,
          topic=command_options['--topic'],
          port=int(command_options['--port']),
          senders=command_options['--senders'],
          recipients=command_options['--recipients'],
          message_names=command_options['--messages'])
          

    def config(self, options, command_options):
        """Validate and view the collector config.

        Usage: config
        """
        pass

    def parse(self, options, command_options):
        """Parse sync message streams into record streams.

        Usage: parse [options]

        Options:
          --source-topic NAME        Topic to consume sync messages from
          --destination-topic NAME   Topic to produce record messages to, defaults
                                     to a topic based on the consumed message name
        """
        pass


def kafka_bootstraps_from_options(options):
  return ','.join(options['--kafka'])
