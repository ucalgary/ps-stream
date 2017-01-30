from twisted.web import resource


class PSSyncReceiver(resource.Resource):

	isLeaf = True

	def render_GET(self, request):
		return '{"status":"GET ok"}'.encode('utf-8')

	def render_POST(self, request):
		return '{"status":"POST ok"}'.encode('utf-8')
