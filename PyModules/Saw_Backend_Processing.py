import csv
import json
import os
from datetime import datetime
from datetime import timedelta
from pylogix.eip import PLC
import zstandard as zstd
from tornado.web import Application,RequestHandler
from tornado.ioloop import IOLoop

def read_current():
    return json.loads(zstd_decompress('.\current.json.zstd'))

def zstd_decompress(fp):
	with open(fp, 'rb') as fo:
		dctx = zstd.ZstdDecompressor()
		decompressed = dctx.decompress(fo.read())
		return decompressed

def get_metric_data(part_num):
	root = 'C:/Part_Logs/Metrics'
	for file in os.listdir(root):
		if part_num.lower() in file.lower():
			with open(os.path.join(root, file), 'r') as js_r:
				return json.load(js_r)
	return {'message': 'record not found'}

def get_hourly_lists(keys):
	d = []
	data_lines = read_current()
	for data in data_lines:
		if isinstance(data, list):
			data = data[0]
		l = {}
		for key in keys:
			l[key] = data['data'][key]
		d.append(l)
	return d

class GetSawDataHandler(RequestHandler):
	def set_default_headers(self, *args, **kwargs):
		self.set_header("Access-Control-Allow-Origin", "*")

	def get(self, *args):
		get_current_data = read_current()
		self.write(json.dumps(get_current_data[len(get_current_data) - 1]))

class GetSawMetricHandler(RequestHandler):
	def set_default_headers(self, *args, **kwargs):
		self.set_header("Access-Control-Allow-Origin", "*")

	def get(self, *args):
		part_num = self.request.path.split('/')[-1]
		metric_data = get_metric_data(part_num)
		self.write(json.dumps(metric_data))

class GetHourlyMetricHandler(RequestHandler):
	def set_default_headers(self, *args, **kwargs):
		self.set_header("Access-Control-Allow-Origin", "*")

	def get(self, *args):
		keys = self.request.path.split('/')[-1].split(',')
		self.write(json.dumps(get_hourly_lists(keys)))

def initialize():
	api_urls = []
	api_urls.append(("/gcs-data", GetSawDataHandler))
	api_urls.append((r"/gcs-metrics/([^/]+)?", GetSawMetricHandler))
	api_urls.append((r"/gcs-hourly/([^/]+)?", GetHourlyMetricHandler))
	return Application(api_urls, debug=True)

if __name__ == '__main__':
	api = initialize()
	api.listen(3010)
	IOLoop.instance().start()
