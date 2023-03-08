import time
import shutil
import codecs
import struct
import math
import threading
import csv
import json
import os
from pathlib import Path
from datetime import datetime
from datetime import timedelta
from pylogix.eip import PLC
import zstandard as zstd

_root = 'C:/Users/data-log/Google Drive'
_constants_previous = {}
	
def ResolveHex(hexValue):
	decode_hex = codecs.getdecoder("hex_codec")
	if isinstance(hexValue, list):
		msgs = [decode_hex(hexValue)[0] for hexValue in msgs]
		return msgs
	else:
		string = decode_hex(hexValue)[0]
		return string

def ResolveHexTwo(hexValue):
	decode_hex = codecs.getdecoder("hex_codec")
	if isinstance(hexValue, list):
		msgs = struct.unpack('<BBH', hexValue)[0] 
		return msgs
	else:
		string = struct.unpack('<BBH', hexValue)
		return string

def get_tag_dictionary(input_tags):
	d = {'array': [], 'non-array':[]}
	for tag_index in input_tags.Value:
		tag_name = tag_index.TagName
		is_array = True if tag_index.Array == 1 else False
		data_type = tag_index.DataType
		if is_array:
			d['array'].append({'tag-name': tag_name, 'data-type': data_type, 'size': tag_index.Size})
		else:
			d['non-array'].append({'tag-name': tag_name, 'data-type': data_type})
	return d

def get_array_values(d):
	array_vals = []
	for array_var in d['array']:
		current_val = saw_comm.Read(array_var['tag-name'], array_var['size'])
		for val in [current_val.Value]:
			if isinstance(val, list):
				if array_var['tag-name'] == 'RECIPE_STORED':
					join_bytes = ''.join([str(x) for x in val]).split('-5')
					r_list = []
					for t_bytes in join_bytes:
						recipe_name = t_bytes[t_bytes.rfind('x0') + 3 : len(t_bytes) - 1]
						r_list.append(recipe_name)
					current_val.Value = set(r_list)
					array_vals.append(current_val)
				else:
					for index in val:
						if not isinstance(index, (bytes, bytearray)):							
							array_vals.append(current_val)
							break
	return array_vals

# Compress the text using Zstandard
def zstd_compress(serialized_data):
	cctx = zstd.ZstdCompressor()
	compressed = cctx.compress(serialized_data.encode('utf-8'))
	return compressed

def zstd_decompress(fp):
	with open(fp, 'rb') as fo:
		dctx = zstd.ZstdDecompressor()
		decompressed = dctx.decompress(fo.read())
		return decompressed

def save_to_minute_json_file(data):
	now = datetime.now()
	fp = os.path.join(_root, 'Saw-Data-V3')
	if not os.path.exists(fp):
		os.mkdir(fp)
	year_path = os.path.join(fp, str(now.year))
	if not os.path.exists(year_path):
		os.mkdir(year_path)
	month_path = os.path.join(year_path, str(now.month))
	if not os.path.exists(month_path):
		os.mkdir(month_path)
	day_path = os.path.join(month_path, str(now.day))
	if not os.path.exists(day_path):
		os.mkdir(day_path)	
	f_name = os.path.join(day_path, 'h-' + str(now.hour) + '_m-' + str(now.minute) + '.json.zstd')
	if os.path.exists(f_name):
		read_existing = zstd_decompress(f_name).decode('utf-8')
		append_to_existing = json.loads(read_existing)
		append_to_existing.append(data)
		compressed_json_data = zstd_compress(json.dumps(append_to_existing))
		with open(f_name, 'wb') as cj_write:
			cj_write.write(compressed_json_data)
	else:	
		with open(f_name, 'wb') as cj_write:
			cj_write.write(zstd_compress(json.dumps(data)))

def save_to_hourly_json_file(data):
	now = datetime.now()
	fp = os.path.join(_root, 'Saw-Data-V3-Constants')
	if not os.path.exists(fp):
		os.mkdir(fp)
	year_path = os.path.join(fp, str(now.year))
	if not os.path.exists(year_path):
		os.mkdir(year_path)
	month_path = os.path.join(year_path, str(now.month))
	if not os.path.exists(month_path):
		os.mkdir(month_path)
	day_path = os.path.join(month_path, str(now.day))
	if not os.path.exists(day_path):
		os.mkdir(day_path)	
	f_name = os.path.join(day_path, 'h-' + str(now.hour) + '_m-' + '.json.zstd')
	if os.path.exists(f_name):
		read_existing = zstd_decompress(f_name).decode('utf-8')
		append_to_existing = json.loads(read_existing)
		append_to_existing.append(data)
		compressed_json_data = zstd_compress(json.dumps(append_to_existing))
		with open(f_name, 'wb') as cj_write:
			cj_write.write(compressed_json_data)
	else:	
		with open(f_name, 'wb') as cj_write:
			cj_write.write(zstd_compress(json.dumps(data)))
	copy_file_to_local_ref(f_name)

def decompressAndSave(d, fp):		
	with open(fp, 'w') as tmp:
		tmp.write(json.dumps(d))

def append_recipe_tags(l):
	suffixes = ['[0]', '.BFR', '.PCL', '.TCP', '.SCP', '.BTW', '.BTH', '.PPC', '.ATD', '.BGP']
	for suffix in suffixes:
		l.append('CURRENT_RECIPE' + suffix)
	return l

def get_saw_constants():
	fp = './PyModules/PrimaryConstants.txt'
	with open(fp, 'r') as sc_read:
		return [x.replace('\n', '') for x in sc_read.readlines()]

def append_to_saw_constants(scalar_lists, c_lists):
	l = []
	for scalar in scalar_lists:
		for constant in c_lists:
			if scalar.startswith(constant[:-4]):
				if scalar not in l:
					l.append(scalar)
	return l

def get_constants_datas(constant_tags, scalar_data, comm):
	d = {}
	cs = comm.Read(constant_tags)
	for c in cs:
		d[c.TagName] = c.Value
	# for c in constant_tags:
	# 	try:
	# 		d[c] = [x.Value for x in scalar_data if x.TagName == c][0]
	# 	except:
	# 		d[c] = comm.Read(c).Value
	return d

def increment_counter_with_subkey(d, time, key, part_name):
	if key not in d:
		d[key] = {}
	hour_key = datetime.fromisoformat(time).hour
	if hour_key not in d[key]:
		d[key][hour_key] = {}
	if hour_key in d[key]:
		d[key][hour_key][part_name] += 1
	else:
		d[key][hour_key][part_name] = 1
	return d

def increment_counter(d, time, key):
	if key not in d:
		d[key] = {}
	hour_key = datetime.fromisoformat(time).hour
	if hour_key in d[key]:
		d[key][hour_key] += 1
	else:
		d[key][hour_key] = 1
	return d

def process_saw_cut_cycle(cp_empty, current_key_time, last_cut_time, theoretical_min_cut_time_in_secs, d_c):
	if d_c['SAWBLADE.ActualPosition'] > 20:
		if cp_empty:
			return current_key_time
		else:
			t_dif = (datetime.fromisoformat(current_key_time) - datetime.fromisoformat(last_cut_time)).total_seconds()
			if t_dif > theoretical_min_cut_time_in_secs:
				return current_key_time
			else:
				return last_cut_time
	else:
		if not cp_empty:
			return False
		else:
			return (datetime.now() - timedelta(seconds=theoretical_min_cut_time_in_secs)).isoformat()

def process_backgauge_reload_cycle(current_key_time, last_backgauge_return_time, min_return_time, cp_empty, d_c):
	if d_c['BACKGAUGE_HOME_POS'] == d_c['BACKGAUGE_ACTUAL_POS']:
		return {'status': "Saw is either currently inactive or in a 'Backgauge Homing Cycle'"}
	elif cp_empty:
		return current_key_time
	else:
		if d_c['BACKGAUGE_ACTUAL_POS'] >= 306 and d_c['BACKGAUGE_ACTUAL_POS'] < 307.5:
			lbgrt = last_backgauge_return_time if isinstance(last_backgauge_return_time, str) else (datetime.fromisoformat(current_key_time) - timedelta(seconds=last_backgauge_return_time)).isoformat()
			t_dif = (datetime.fromisoformat(current_key_time) - datetime.fromisoformat(lbgrt)).total_seconds()
			if t_dif >= min_return_time:
				return current_key_time
			else:
				return last_backgauge_return_time
		else:
			return last_backgauge_return_time

def generate_measureables_from_constants(d_c, data_to_save):
	global _constants_previous
	d = {}
	cp_empty = True if len(list(_constants_previous.keys())) == 0 else False
	if 'time' in d_c and 'data' in d_c:
		data_collection_time_stamp = d_c['time']
		data = d_c['data']
		[part_name, cut_length, cut_length_in_inches] = [data['CURRENT_RECIPE'], data['CURRENT_RECIPE.PCL'], data['CURRENT_RECIPE.PCL'] / 25.4]
		[backgauge_position, next_backgauge_position] = [data['BACKGAUGE_ACTUAL_POS'], data['NEXT_BACKGAUGE_POS']]
		theoretical_min_cut_time_in_secs = ((data['SIDECLAMP_POS'] + 10) / data['C_AUTO_FEED_DISP']) * 60
		cuts_remaining = math.floor(data['BACKGAUGE_ACTUAL_POS']/ cut_length_in_inches if cut_length_in_inches - 1 < (data['BACKGAUGE_ACTUAL_POS'] - data['NEXT_BACKGAUGE_POS']) else data['NEXT_BACKGAUGE_POS'])
		theoretical_min_return_time_in_secs = cuts_remaining * theoretical_min_cut_time_in_secs + 15

		d[part_name] = {}
		# Things we want to know/measure that are derived...
		# The time of each saw cut
		# The time of each batch load
		# The time time of the last saw cut
		# The time of the last batch load
		# The dead cycle time
		# Other things can be further derived from this... i.e. how long should a job take, efficiency metrics, etc.

		# Time Of Each Saw Cut
		last_cut_time = (datetime.now() - timedelta(seconds=theoretical_min_cut_time_in_secs)).isoformat()
		last_batch_load = (datetime.now() - timedelta(seconds=theoretical_min_return_time_in_secs)).isoformat()
		if 'derived' in _constants_previous:
			if part_name in _constants_previous['derived']:
				if 'saw-cut-times' in _constants_previous['derived'][part_name]:
					if len(_constants_previous['derived'][part_name]['saw-cut-times']) > 0:
						last_cut_time = _constants_previous['derived'][part_name]['saw-cut-times'][len(_constants_previous['derived'][part_name]['saw-cut-times']) - 1]
		else:
			last_cut_time = (datetime.now() - timedelta(seconds=theoretical_min_cut_time_in_secs)).isoformat()

		if 'derived' in _constants_previous:
			if part_name in _constants_previous['derived']:
				if 'batch-load-times' in _constants_previous['derived'][part_name]:
					if len(_constants_previous['derived'][part_name]['batch-load-times']) > 0:
						last_batch_load = _constants_previous['derived'][part_name]['batch-load-times'][len(_constants_previous['derived'][part_name]['batch-load-times']) - 1]
		else:
			last_batch_load = (datetime.now() - timedelta(seconds=theoretical_min_return_time_in_secs)).isoformat()
		
		d[part_name]['saw-cut-times'] = process_saw_cut_cycle(cp_empty, data_collection_time_stamp, theoretical_min_cut_time_in_secs=theoretical_min_cut_time_in_secs, last_cut_time=last_cut_time, d_c=data)
		d[part_name]['batch-load-times'] = process_backgauge_reload_cycle(current_key_time=data_collection_time_stamp, last_backgauge_return_time=last_batch_load, min_return_time=theoretical_min_return_time_in_secs, cp_empty=cp_empty, d_c=data)

		data_to_save['data'] = data
		data_to_save['time'] = data_collection_time_stamp
		if cp_empty:
			data_to_save['derived'] = {}
			data_to_save['derived'][part_name] = {}
			data_to_save['derived'][part_name]['saw-cut-times'] = []
			data_to_save['derived'][part_name]['batch-load-times'] = []
		elif part_name not in data_to_save['derived']:
			data_to_save['derived'][part_name] = {}
			data_to_save['derived'][part_name]['saw-cut-times'] = []
			data_to_save['derived'][part_name]['batch-load-times'] = []
		else:
			if d[part_name]['saw-cut-times'] not in data_to_save['derived'][part_name]['saw-cut-times']:
				if d[part_name]['saw-cut-times'] != False:
					data_to_save['derived'][part_name]['saw-cut-times'].append(d[part_name]['saw-cut-times'])
			if d[part_name]['batch-load-times'] not in data_to_save['derived'][part_name]['batch-load-times']:
				if d[part_name]['batch-load-times'] != False:
					data_to_save['derived'][part_name]['batch-load-times'].append(d[part_name]['batch-load-times'])
		data_to_save = check_for_daily_overflow(data_to_save)
		return data_to_save

	else:
		return d_c

def check_for_daily_overflow(data):
	current_part = data['data']['CURRENT_RECIPE[0]']
	keys_to_pop = []
	for key in data['derived']:
		if key != current_part:
			save_daily_part_log(data['derived'][key], key)
			keys_to_pop.append(key)
	for k in keys_to_pop:
		data['derived'].pop(k)
	return data

def save_daily_part_log(data, pn):
	rp = '/Part_Logs'
	d = {}
	now = datetime.now()
	key_string = str(now.month) + '-' + str(now.day) + '-' + str(now.year)
	if not os.path.exists(rp):
		os.mkdir(rp)
	fp = os.path.join(rp, pn + '.json')
	if os.path.exists(fp):
		with open(fp, 'r') as js_read:
			d = json.load(js_read)
			d[key_string] = data
	else:
		d[key_string] = data
	with open(fp, 'w') as js_write:
		json.dump(d, js_write)

def copy_file_to_local_ref(file):
	try:
		shutil.copyfile(file, './current.json.zstd')
	except:
		_=0

def get_last_data_from_saved(day_offset):
	now = datetime.now() - timedelta(days= day_offset)
	fp = os.path.join(_root, 'Saw-Data-V3-Constants')
	if not os.path.exists(fp):
		os.mkdir(fp)
	year_path = os.path.join(fp, str(now.year))
	if not os.path.exists(year_path):
		os.mkdir(year_path)
	month_path = os.path.join(year_path, str(now.month))
	if not os.path.exists(month_path):
		os.mkdir(month_path)
	day_path = os.path.join(month_path, str(now.day))
	if not os.path.exists(day_path):
		os.mkdir(day_path)	
	paths = [os.path.join(day_path, x) for x in os.listdir(day_path)]
	paths.sort(key=lambda f: os.path.getmtime(os.path.join(day_path, f)))
	if len(paths) == 0:
		day_offset += 1
		get_last_data_from_saved(day_offset)
	else:
		last_file = paths[len(paths) - 1]
		last_write_time = datetime.fromtimestamp(os.path.getmtime(last_file))
		time_diff_in_mins = (now - last_write_time).total_seconds() / 60
		if time_diff_in_mins < 60:
			dczstd = zstd_decompress(last_file)
			jso_rdr = json.loads(dczstd)
			return jso_rdr[len(jso_rdr) - 1]
		else:
			return {}

# def process_constants_datas(d_current):
# 	global _constants_previous
# 	d_send = {}
# 	[part_name, cut_length] = [d_current['data']['CURRENT_RECIPE'], d_current['data']['CURRENT_RECIPE.PCL']]
# 	cp_empty = True if len(list(_constants_previous.keys())) == 0 else False
# 	[last_backgauge_reload_time, last_cut_time, backgauge_loads_this_hour, cuts_this_hour] = ([datetime.now().isoformat(), datetime.now().isoformat(), 0, 0] 
# 	if cp_empty 
# 	else [
# 	_constants_previous['data']['last-backgauge-reload-time'], 
# 	_constants_previous['data']['last-cut-time'], 
# 	_constants_previous['data']['']['last-backgauge-reload-time'], 
# 	_constants_previous['data']['']['last-backgauge-reload-time']])
# 	current_key_time = d_current['time']
# 	_constants_previous_key_time = datetime.now().isoformat() if cp_empty else _constants_previous['time']
# 	d_c = d_current['data']
# 	theoretical_min_cut_time_in_secs = ((d_c['SIDECLAMP_POS'] + 10) / d_c['C_AUTO_FEED_DISP']) * 60
# 	if cp_empty:
# 		d_send['dead-cycle-time'] = 0
# 	else:
# 		bcg_unchanged = d_c['BACKGAUGE_ACTUAL_POS'] == _constants_previous['data']['BACKGAUGE_ACTUAL_POS']
# 		saw_unchanged = d_c['SAWBLADE.ActualPosition'] == _constants_previous['data']['SAWBLADE.ActualPosition']
# 		if bcg_unchanged and saw_unchanged:
# 			increment_counter_with_subkey(d_send, current_key_time, 'dead-cycle-time', part_name)
# 	for key in d_current['data']:
# 		d_send[key] = d_current['data'][key]
# 	_constants_previous = {'time': current_key_time, 'data': d_send}
# 	return {'time': current_key_time, 'data': d_send}


saw_comm = PLC()
saw_comm.IPAddress = "10.4.20.21"
parse_saw_tags = get_tag_dictionary(saw_comm.GetTagList())
list_scalar_tags = append_recipe_tags([x['tag-name'] for x in parse_saw_tags['non-array']])
constant_tags = append_to_saw_constants(list_scalar_tags, get_saw_constants())
constant_tags.extend(['SAWBLADE.HomeEventStatus', 'SAWBLADE.ActualPosition'])
serializable_data_types = (float, int, str, list, bool)
constants_previous = get_last_data_from_saved(0)
_constants_previous = constants_previous[0] if bool(constants_previous) else {}
while True:
	now = datetime.now()
	#scalar_data = saw_comm.Read(list_scalar_tags)
	#array_data = get_array_values(parse_saw_tags)
	d_c = get_constants_datas(constant_tags, [], saw_comm)
	d_constants = [{'time': datetime.now().isoformat(), 'data': d_c}]
	#d = {'scalar-saw-data': [{x.TagName: x.Value if isinstance(x.Value, serializable_data_types) else str(x.Value)} for x in scalar_data], 'array-saw-data': [{y.TagName: y.Value if isinstance(y.Value, serializable_data_types) else str(y.Value)} for y in array_data]}
	#d_final = [{'time': datetime.now().isoformat(), 'data': d}]
	p_constants = generate_measureables_from_constants(d_constants[0], _constants_previous)
	_constants_previous = p_constants
	#save_to_minute_json_file(d_final)
	save_to_hourly_json_file([p_constants])
	execution_time = (datetime.now() - now).total_seconds()
	pause_time = 0.99 - execution_time if execution_time < 0.99 else 0 
	time.sleep(pause_time)
	print((datetime.now() - now).total_seconds())

	# dc_belt_started_tn = 'DISCHARGE_CONV_START'
	# dc_belt_stopped_tn = 'DISCHARGE_CONV_STOP'
	# dc_belt_speed_tn = 'DISCHARGE_CONVEYOR_SPEED'
	# recipe_name_tn = 'CURRENT_RECIPE'
	# current_recipe_name_tn = recipe_name_tn + '[0]'
	# recipe_blade_feed_rate_tn = recipe_name_tn + '.BFR' #Blade Feed Rate
	# recipe_current_length_tn = recipe_name_tn + '.PCL' #Cut Length Setpoint
	# recipe_current_top_clamp_prs_tn = recipe_name_tn + '.TCP' #Top Clamp Pressure
	# recipe_current_side_clamp_prs_tn = recipe_name_tn + '.SCP' #Side Clamp Pressure
	# recipe_current_batch_width_tn = recipe_name_tn + '.BTW' #Batch Width
	# recipe_current_batch_height_tn = recipe_name_tn + '.BTH' #Batch Height
	# recipe_current_parts_per_cut_tn = recipe_name_tn + '.PPC' #Piece per Cut
	# recipe_current_auto_trim_dist_tn = recipe_name_tn + '.ATD' #Auto Trim Distance
	# recipe_current_back_gage_prs_tn = recipe_name_tn + '.BGP' #Back Gauge Pressure
	# actual_back_gauge_position = 'BACKGAUGE_ACTUAL_POS'
	# is_back_gauge_reverse = 'B_G_RELOAD_CYCLE'
	# actual_blade_feed_rate = 'C_AUTO_FEED_DISP'
	# actual_current_length = 'C_CUT_LEN_DISP'
	# actual_current_top_clamp_prs = 'C_VERT_PRESS_DISP'
	# actual_side_clamp_prs = 'C_HORIZ_PRESS_DISP'
	# actual_batch_width = 'C_BAT_WIDTH_DISP'
	# actual_batch_height = 'C_BAT_HEIGHT_DISP'
	# actual_parts_per_cut = 'C_PTS_PER_CUT_DISP'
	# actual_auto_trim_dist = 'C_AUTO_TRIM_DISP'
	# actual_back_gage_prs = 'C_BACKGAUGE_PRESS_DISP'
	# current_cut_quantity = 'ACTUAL_QTY_DISP'
	# is_batch_load_start = 'BATCH_LOADED_START'
	# back_gauge_home_position = 'BACKGAUGE_HOME_POS'
	# next_back_gauge_position = 'NEXT_BACKGAUGE_POS'
	# auto_mode_enabled = 'AUTO_MODE'
	# trim_cut_slowdown = 'TRIM_CUT_SLOW_DIST'
	# saw_blade_home_status = 'SAWBLADE.HomeEventStatus'
	# saw_blade_position = 'SAWBLADE.ActualPosition'
