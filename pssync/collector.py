import json

from xml.etree import ElementTree

from twisted.internet import endpoints, reactor
from twisted.web import resource, server

from .utils import element_to_obj

class PSSyncCollector(resource.Resource):

    isLeaf = True

    def __init__(self, producer, topic=None):
        super().__init__()
        self.producer = producer
        self.topic = topic

    def render_GET(self, request):
        return '{"status":"GET ok"}'.encode('utf-8')

    def render_POST(self, request):
        """Decode PeopleSoft rowset-based messages into transactions, and produce Kafka
        messages for each transaction. PeopleSoft is expected to POST messages as events
        occur via SYNC and FULLSYNC services.

        The following URL describes the PeopleSoft Rowset-Based Message Format.
        http://docs.oracle.com/cd/E66686_01/pt855pbr1/eng/pt/tibr/concept_PeopleSoftRowset-BasedMessageFormat-0764fb.html
        """
        psft_message_name = None
        field_types = None

        # Parse the root element for the PeopleSoft message name and FieldTypes
        request.content.seek(0,0)
        for event, e in ElementTree.iterparse(request.content, events=('start', 'end')):
            if event == 'start' and psft_message_name is None:
                psft_message_name = e.tag
            elif event == 'end' and e.tag == 'FieldTypes':
                field_types = element_to_obj(e, value_f=field_type)
                break

        # Rescan for transactions, removing read elements to reduce memory usage
        request.content.seek(0,0)
        for event, e in ElementTree.iterparse(request.content, events=('end',)):
            if e.tag == 'Transaction':
                print(json.dumps(element_to_obj(e), indent=4))
                e.clear()

        return '{"status":"POST ok"}'.encode('utf-8')


def collect(producer, topic=None, port=8000, senders=None, recipients=None, message_names=None):
    collector = PSSyncCollector(producer, topic=topic)
    site = server.Site(collector)
    endpoint = endpoints.TCP4ServerEndpoint(reactor, int(port))
    endpoint.listen(site)
    print(f'Listening for connections on port {port}')
    reactor.run()


def field_type(element):
    assert('type' in element.attrib)
    return element.attrib.get('type')
