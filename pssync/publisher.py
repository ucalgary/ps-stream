class PSSyncPublisher(object):

    def __init__(self, consumer, producer, source_topics=None, destination_topic=None):
        super().__init__()
        self.consumer = consumer
        self.producer = producer
        self.source_topics = source_topics
        self.destination_topic = destination_topic
        self.running = True

    def run(self):
        pass


def publish(consumer, producer, source_topics=None, destination_topic=None):
    publisher = PSSyncPublisher(consumer, producer, source_topics=None, destination_topic=None)
    print(f'Reading transactions from {source_topics}')
    publisher.run()
