from twisted.internet import reactor, endpoints
from twisted.web import resource, server


class PSSyncCollector(resource.Resource):

	isLeaf = True

	def render_GET(self, request):
		return '{"status":"GET ok"}'.encode('utf-8')

	def render_POST(self, request):
		print(request.getAllHeaders())
		print(request.content.read())
		return '{"status":"POST ok"}'.encode('utf-8')


if __name__ == '__main__':
	site = server.Site(PSSyncCollector())
	endpoint = endpoints.TCP4ServerEndpoint(reactor, 8080)
	endpoint.listen(site)
	reactor.run()
