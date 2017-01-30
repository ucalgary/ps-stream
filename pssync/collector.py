#!/usr/bin/env python3

import functools
import logging
import sys

from docopt import docopt
from inspect import getdoc
from twisted.internet import reactor, endpoints
from twisted.web import resource, server

from .cli.docopt_command import DocoptDispatcher, NoSuchCommand


log = logging.getLogger(__name__)


class PSSyncCommand(object):
	"""Collect PeopleSoft sync messages into Kafka topics.

	Usage:
	  ps-sync-collector [options] [COMMAND] [ARGS...]
	  ps-sync-collector -h|--help

	Options:
	  -b, --broker BROKER_HOSTS     Comma-separated list of Kafka broker hosts (default: kafka:9092)
	  -r, --registry REGISTRY_HOST  URL of the service where Avro schemas are registered
	  -p, --topic-prefix PREFIX     String to prepend to all topic names

	Commands:
	  collect            Collect PeopleSoft sync messages
	  config             Validate and view the collector config
	"""

	def collect(self, options, command_options):
		"""
		Collect PeopleSoft sync and fullsync messages.

		Usage: collect
		"""
		site = server.Site(PSSyncCollector())
		endpoint = endpoints.TCP4ServerEndpoint(reactor, 8080)
		endpoint.listen(site)
		reactor.run()

	def config(self, options, command_options):
		"""
		Validate and view the collector config

		Usage: config
		"""
		pass


class PSSyncCollector(resource.Resource):

	isLeaf = True

	def render_GET(self, request):
		return '{"status":"GET ok"}'.encode('utf-8')

	def render_POST(self, request):
		print(request.getAllHeaders())
		print(request.content.read())
		return '{"status":"POST ok"}'.encode('utf-8')


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


if __name__ == '__main__':
	main()
