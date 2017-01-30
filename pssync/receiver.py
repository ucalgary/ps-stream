from twisted.web import resource


class PSSyncReceiver(resource.Resource):

	isLeaf = True

	def render_GET(self, request):
		return '{"status":"ok"}'.encode('utf-8')
