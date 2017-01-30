from twisted.web import resource, server


class PSSyncCollector(resource.Resource):

	isLeaf = True

	def render_GET(self, request):
		return '{"status":"GET ok"}'.encode('utf-8')

	def render_POST(self, request):
		print(request.getAllHeaders())
		print(request.content.read())
		return '{"status":"POST ok"}'.encode('utf-8')
