import functools
import logging
import sys

from docopt import docopt
from inspect import getdoc
from twisted.internet import endpoints
from twisted.internet import reactor
from twisted.web import server

from ..collector import PSSyncCollector
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
	  pssync [options] [COMMAND] [ARGS...]
	  pssync -h|--help

	Options:
	  -z, --zookeeper HOST          Zookeeper service discoveryhost [default: zookeeper:2181]
	  -k, --kafka HOSTS             Kafka broker hosts, in lieu of zookeeper [default: kafka:9092]
	  -r, --schema-registry URL     Avro schema registry host [default: schema-registry:80]
	  -p, --topic-prefix PREFIX     String to prepend to all topic names

	Commands:
	  collect            Collect PeopleSoft sync messages
	  config             Validate and view the collector config
	  parse              Parse sync message streams into record streams
	"""

	def collect(self, options, command_options):
		"""Collect PeopleSoft sync and fullsync messages.

		Usage: collect [options]

		Options:
		  --port PORT                Port to listen to messages on [default: 8000]
		  --sender-name NAMES        Accepted values for the From header
		  --recipient-name NAMES     Accepted values for the To header
		  --message-name NAMES       Accepted values for the MessageName header
		  --producer-topic TOPIC     Produce to a specific Kafka topic, otherwise
		                             messages are sent to topics by message name
		"""
		

		site = server.Site(PSSyncCollector())
		endpoint = endpoints.TCP4ServerEndpoint(reactor, 8080)
		endpoint.listen(site)
		reactor.run()

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
