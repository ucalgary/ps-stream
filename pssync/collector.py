from twisted.internet import endpoints, reactor
from twisted.web import resource, server


class PSSyncCollector(resource.Resource):

    isLeaf = True

    def render_GET(self, request):
        return '{"status":"GET ok"}'.encode('utf-8')

    def render_POST(self, request):
        print(request.getAllHeaders())
        print(request.content.read())
        return '{"status":"POST ok"}'.encode('utf-8')


def collect(producer, topic=None, port=8000, senders=None, recipients=None, message_names=None):
    collector = PSSyncCollector()
    site = server.Site(collector)
    endpoint = endpoints.TCP4ServerEndpoint(reactor, int(port))
    endpoint.listen(site)
    reactor.run()
