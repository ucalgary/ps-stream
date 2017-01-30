from twisted.internet import reactor, endpoints
from twisted.web import resource, server


class PSSyncReceiver(resource.Resource):

	isLeaf = True

	def render_GET(self, request):
		return '{"status":"GET ok"}'.encode('utf-8')

	def render_POST(self, request):
		return '{"status":"POST ok"}'.encode('utf-8')


if __name__ == '__main__':
	site = server.Site(PSSyncReceiver())
	endpoint = endpoints.TCP4ServerEndpoint(reactor, 8080)
	endpoint.listen(site)
	reactor.run()
