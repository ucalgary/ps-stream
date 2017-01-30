from twisted.internet import reactor, endpoints
from twisted.web import server

from .receiver import PSSyncReceiver

site = server.Site(PSSyncReceiver())
endpoint = endpoints.TCP4ServerEndpoint(reactor, 8080)
endpoint.listen(site)
reactor.run()
