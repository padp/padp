import urllib.request
import subprocess
import wget
import zipfile
import os

_root_dir = 'C:/Users/data-log/Desktop/Scrap Scrape Data/ScrapScrape_v1/ScrapScrape_v1/bin/Debug/netcoreapp3.1'

def get_chrome_version():
    output = subprocess.check_output(
        r'wmic datafile where name="C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe" get Version /value',
        shell=True
    )
    try:
        version_string = output.decode('utf-8').strip()
        return {version_string.split('=')[0]: version_string.split('=')[1]}
    except:
        return {'failure': 'no version detected'}
    
def parse_versions_from_url():
    l = []
    with urllib.request.urlopen('https://chromedriver.storage.googleapis.com/') as response:
        html = response.read().decode('utf-8')
        html_split_key = html.split('<Key>')
        for vn in html_split_key:
            version = vn.split('</Key>')[0]
            l.append(version)
    return l

def get_numeric_version_map(input_str, delimiter):
    split_str = input_str.split(delimiter)[0].split('.') if delimiter != '' else input_str.split('.')
    d = {}
    try:
        for val in enumerate(split_str):
            d[val[0]] = int(val[1])
    except:
       return 'error'
    return d

def check_remaining_version_infos_same_root(local_map, comparer_map):
    if comparer_map[1] <= local_map[1]:
        if comparer_map[2] <= local_map[2]:
            if comparer_map[3 <= local_map[3]]:
                return True
    return False

urls = {'primary':[], 'secondary':[]}
local_chrome_version = get_chrome_version()
chromedriver_versions = parse_versions_from_url()
local_version_map = get_numeric_version_map(local_chrome_version['Version'], '')
for t in chromedriver_versions:
    v_map = get_numeric_version_map(t, '/')
    if v_map != 'error':
        if v_map[0] == local_version_map[0]:
            if check_remaining_version_infos_same_root:
                urls['primary'].append(t)
        elif v_map[0] == local_version_map[0] - 1:
            urls['secondary'].append(t)

prefferred_versions_query_address = [x for x in urls['primary'] if 'win32' in x]
lastest_version = prefferred_versions_query_address[len(prefferred_versions_query_address) - 1]
download_url = "https://chromedriver.storage.googleapis.com/" + lastest_version

latest_driver_zip = wget.download(download_url,'chromedriver.zip')
os.system("taskkill /f /im  chromedriver.exe")
# extract the zip file
with zipfile.ZipFile(latest_driver_zip, 'r') as zip_ref:
    zip_ref.extractall(_root_dir) # you can specify the destination folder path here
# delete the zip file downloaded above
os.remove(latest_driver_zip)
