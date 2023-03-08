import csv
import json
import os
from datetime import datetime
from datetime import timedelta
from pylogix.eip import PLC
import zstandard as zstd
import math
import statistics

_root_dir = 'C:\Part_Logs'
_data_dir = 'C:/Users/data-log/Google Drive/Saw-Data-V3-Constants'
_dest_dir = 'C:/Part_Logs/Metrics'

def get_files_lists():
    l = []
    for file in os.listdir(_root_dir):
        if '.json' in file:
            if len(file) > len('.json'):
                l.append({'part': file.rstrip('.json'), 'fp': os.path.join(_root_dir, file)})
    return l

def zstd_decompress(fp):
	with open(fp, 'rb') as fo:
		dctx = zstd.ZstdDecompressor()
		decompressed = dctx.decompress(fo.read())
		return decompressed

def read_file_into_object(fp):
    with open(fp, 'r') as js_read:
        return json.load(js_read)

def get_associated_data_files(js_obj):
    lfs = []
    for key in js_obj:
        lfi = []
        [month,day,year] = [str(datetime.fromisoformat(js_obj[key]['saw-cut-times'][0]).month), str(datetime.fromisoformat(js_obj[key]['saw-cut-times'][0]).day), str(datetime.fromisoformat(js_obj[key]['saw-cut-times'][0]).year)]
        t_dir = os.path.join(_data_dir, year, month, day)
        if os.path.exists(t_dir):
            start_time = datetime.fromisoformat(js_obj[key]['saw-cut-times'][0])
            end_time = datetime.fromisoformat(js_obj[key]['saw-cut-times'][len(js_obj[key]['saw-cut-times']) - 1])
            range_hrs = list(range(start_time.hour, end_time.hour + 1))
            for range_hr in range_hrs:
                fi_starts = 'h-' + str(range_hr)
                tl = [os.path.join(t_dir, x) for x in os.listdir(t_dir) if x.startswith(fi_starts)]
                for dfs in tl:
                    if dfs not in lfi:
                        lfi.append(dfs)
        lfs.append(lfi)
    return lfs
           
def read_associated_data_files(data_fis):
    l = []
    for file_group in data_fis:
        d = {}
        for fi in file_group:
            decompress_fi = zstd_decompress(fi)
            d[fi] = json.loads(decompress_fi)
        l.append(d)
    return l

def get_saw_multiple(js_obj, data_file_lists):
    data_files = data_file_lists[0]
    for key in js_obj:
        first_cut_time = js_obj[key]['saw-cut-times'][0]
        for data_key in data_files:
            if isinstance(data_files[data_key][0], dict):
                data_files[data_key][0] = [data_files[data_key][0]]
            first_cut_data = [x[1] for x in enumerate(data_files[data_key]) if datetime.fromisoformat(data_files[data_key][x[0]][0]['time']) == datetime.fromisoformat(first_cut_time)]
            if len(first_cut_data) > 0:
                part_length_in_inches = first_cut_data[0][0]['data']['C_CUT_LEN_DISP'] / 25.4
                current_bg_position = first_cut_data[0][0]['data']['BACKGAUGE_ACTUAL_POS']
                cut_multiple = math.floor(current_bg_position / part_length_in_inches)
                return cut_multiple
        
        num_cuts = len(js_obj[key]['saw-cut-times'])
        num_batches = len(js_obj[key]['batch-load-times'])
        estimated_multiple = num_cuts / num_batches
        return math.ceil(estimated_multiple)

def get_expected_file_name(cut_time_iso):
    dt_ft = datetime.fromisoformat(cut_time_iso)
    fn = os.path.join(_data_dir, str(dt_ft.year), str(dt_ft.month), str(dt_ft.day), 'h-' + str(dt_ft.hour) + '_m-.json.zstd')
    if os.path.exists(fn):
        return fn
    else:
        return ''

def verify_object_times(j_obj, key, subkey):
    v_list = []
    for item in j_obj[key][subkey]:
        j_obj_date = datetime.fromisoformat(item)
        j_mdy = str(j_obj_date.month) + '-' + str(j_obj_date.day) + '-' + str(j_obj_date.year)
        if j_mdy == key:
            return j_obj
    for item in j_obj[key][subkey]:
        v_list.append((datetime.fromisoformat(item) + timedelta(days=1)).isoformat())
    j_obj[key][subkey] = v_list        

def segregate_data_parsing(data_file, first_cut_data, last_cut_data):
    d = []
    for data_file_key in data_file[1]:
        try:
            t_obj = data_file[1][data_file_key]
            for line_item in t_obj:
                if 'time' in line_item[0]:
                    if datetime.fromisoformat(line_item[0]['time']).timestamp() >= datetime.fromisoformat(first_cut_data[0][0]['time']).timestamp():
                        if datetime.fromisoformat(line_item[0]['time']) > datetime.fromisoformat(last_cut_data[0][0]['time']):
                            return d
                        d.append(line_item[0])
        except:
            _=0
    return d

def approx_equivalent_dates(date_one, date_two):
    calc = math.sqrt(math.pow((date_one - date_two).total_seconds(), 2))
    if calc < 1:
        return True
    else:
        return False

def get_saw_expectations(js_obj, data_file_lists):
    l = []
    for data_file in enumerate(data_file_lists):
        if data_file[1] == {}:
            continue
        key = list(js_obj.keys())[data_file[0]]
        first_cut_time = js_obj[key]['saw-cut-times'][0]
        last_cut_time = js_obj[key]['saw-cut-times'][-1]
        for data_key in data_file[1]:
            if isinstance(data_file[1][data_key][0], dict):
                data_file[1][data_key][0] = [data_file[1][data_key][0]]           
        first_cut_key = get_expected_file_name(first_cut_time)
        last_cut_key = get_expected_file_name(last_cut_time)
        first_cut_data = [x[1] for x in enumerate(data_file[1][first_cut_key]) if approx_equivalent_dates(datetime.fromisoformat(data_file[1][first_cut_key][x[0]][0]['time']), datetime.fromisoformat(first_cut_time))] 
        last_cut_data = [x[1] for x in enumerate(data_file[1][last_cut_key]) if approx_equivalent_dates(datetime.fromisoformat(data_file[1][last_cut_key][x[0]][0]['time']), datetime.fromisoformat(last_cut_time))]
        if len(first_cut_data) > 0:
            _=0
        if len(last_cut_data) > 0:
            _=0
        l.append(segregate_data_parsing(data_file, first_cut_data, last_cut_data))
    return l

def process_saw_data(saw_data_lists, multiple, part_num):
    d = {part_num: []}
    for saw_datas in saw_data_lists:
        if len(saw_datas) > 0:
            const_bg_return_secs = 70
            start_time = saw_datas[0]['time']
            end_time = saw_datas[-1]['time']
            blade_feed_rate = [x['data']['C_AUTO_FEED_DISP'] for x in saw_datas]
            blade_positions = [x['data']['SAWBLADE.ActualPosition'] for x in saw_datas]
            derived_saw_cuts = saw_datas[-1]['derived'][part_num]['saw-cut-times']
            derived_batch_loads = saw_datas[-1]['derived'][part_num]['batch-load-times']
            q_bfr = statistics.quantiles(blade_feed_rate)
            q_bpos = statistics.quantiles(blade_positions)
            total_ideal_batch_load_time = len(derived_batch_loads) * const_bg_return_secs
            ideal_cut_time = (((((45 - q_bpos[1]) * multiple) + (45)) / q_bfr[1]) * 60) / multiple
            total_ideal_cut_time = ideal_cut_time * len(derived_saw_cuts)
            total_ideal_job_time = total_ideal_cut_time + total_ideal_batch_load_time
            scaled_uptime = ((8 * 60) - 30 - 10 - 30) / (8 * 60)
            actual_job_time = (datetime.fromisoformat(saw_datas[-1]['time']) - datetime.fromisoformat(saw_datas[0]['time'])).total_seconds()
            efficiency = (total_ideal_job_time / actual_job_time) / scaled_uptime
            d[part_num].append({'part-number': part_num, 'start-time': start_time, 'end-time': end_time, 'blade-feed-rate': q_bfr[1], 'start-blade-position': q_bpos[1], 'efficiency': efficiency, 'multiple': multiple})
    return d

files_to_reads = get_files_lists()
for file in files_to_reads:
    try:
        part_number = file['part']
        fp = os.path.join(_dest_dir, part_number + '.json')
        if not os.path.exists(fp):
            js_data = read_file_into_object(file['fp'])
            associated_data_files = read_associated_data_files(get_associated_data_files(js_data))
            multiple = get_saw_multiple(js_data, associated_data_files)
            saw_calcs = process_saw_data(get_saw_expectations(js_data, associated_data_files), multiple, part_number)
            fp = os.path.join(_dest_dir, part_number + '.json')
            with open(fp, 'w') as js_write:
                json.dump(saw_calcs, js_write)
    except:
        _=0
