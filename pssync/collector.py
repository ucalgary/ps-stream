import logging
from twisted.internet import reactor, endpoints
from twisted.web import resource, server



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

	def collect(self, options):
		"""
		Collect PeopleSoft sync and fullsync messages.

		Usage: collect
		"""
		log.info('collect')
		log.info(options)

		site = server.Site(PSSyncCollector())
		endpoint = endpoints.TCP4ServerEndpoint(reactor, 8080)
		endpoint.listen(site)
		reactor.run()

	def config(self, options):
		"""
		Validate and view the collector config

		Usage: config
		"""
		log.info('config')
		log.info(options)


class PSSyncCollector(resource.Resource):

	isLeaf = True

	def render_GET(self, request):
		return '{"status":"GET ok"}'.encode('utf-8')

	def render_POST(self, request):
		print(request.getAllHeaders())
		print(request.content.read())
		return '{"status":"POST ok"}'.encode('utf-8')


def main():
	site = server.Site(PSSyncCollector())
	endpoint = endpoints.TCP4ServerEndpoint(reactor, 8080)
	endpoint.listen(site)
	reactor.run()

if __name__ == '__main__':
	main()
