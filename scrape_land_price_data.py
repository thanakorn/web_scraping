import os
import re
import pandas as pd

from urllib import parse, request
from html.parser import HTMLParser

land_url = 'http://property.treasury.go.th/pvmwebsite/search_data/s_land1_result.asp'
ns3_url = 'http://property.treasury.go.th/pvmwebsite/search_data/s_ns3a_result.asp'

price_url = 'http://property.treasury.go.th/pvmwebsite/search_data/r_land_price.asp?landid=%d&changwat=%d&amphur=%d'
ns3data_url = 'http://property.treasury.go.th/pvmwebsite/search_data/r_ns3a_price.asp?ns3aid=%d'

excluded = {
    '.::: ระบบเว็บไซต์ให้บริการประชาชน :::. กรมธนารักษ์',
    'บัญชีราคาประเมินทุนทรัพย์ที่ดิน', 'สำนักงานที่ดินจังหวัด', 'สาขา', 'รอบบัญชี พ.ศ.', 
    'โฉนดเลขที่ :', 'อำเภอ :', 'หน้าสำรวจ :', 'ตำบล :',
    'เครื่องหมายที่ดิน',
    'ระวาง :', 'แผ่นที่ :', 'มาตราส่วน :', 'เลขที่ดิน :', 'โซน :', 'บล็อก :', 'ล็อท/หน่วย :',
    'เนื้อที่(ไร่-งาน-ตร.วา) :', 'ราคาประเมิน (บาท ต่อ ตร.วา) :', 'บาท',
    'สำนักงานที่ดิน จังหวัด', 'เลขที่ นส.3ก :', 'ชื่อระวางรูปถ่ายทางอากาศ :', 'หมายเลขระวาง :'
}
land_column_names = ['จังหวัด', 'สาขา', 'รอบบช.', 'โฉนด', 'อำเภอ', 'หน้าสำรวจ', 'ตำบล', 'ระวาง', 'ระวาง2', 'แผนที่', 'มาตราส่วน', 'เลขที่ดิน', 'เนื้อที่', 'ราคา']
ns3a_column_names = ['จังหวัด', 'สาขา', 'รอบบช.', 'เลขที่ นส.3ก', 'อำเภอ', 'ตำบล', 'ชื่อระวางรูปถ่ายทางอากาศ', 'หมายเลขระวาง', 'แผ่นที่', 'เลขที่ดิน', 'มาตราส่วน','เนื้อที่','ราคา']

class DataParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.output = ''
    
    def handle_data(self, data):
        data = data.strip('\\r\\n').replace('\xa0', ' ').strip()
        if not data.startswith('<!--') and len(data) > 0 and data not in excluded:
            self.output += ';' + str(data.strip())

def get_land_data(land_no, survey_no, province_no):
    # Extract Land Id
    land_request_data = {
                            'chanode_no': land_no,
                            'survey_no': survey_no,
                            'selChangwat': province_no
                        }
    land_request_body = parse.urlencode(land_request_data).encode()
    land_request = request.Request(land_url, land_request_body)
    land_response = request.urlopen(land_request)
    landprice_params = re.search('LandReport\(\d+,\d+,\S+\)', str(land_response.read())).group(0)
    params = re.search('\(\S+\)', landprice_params).group(0)[1:-2]
    params = [int(re.search('\d+', id).group(0)) for id in params.split(',')]
    land_id, province_id, amphur_id = params[0], params[1], params[2]
    
    # Scrape Land data
    landprice_response = request.urlopen(price_url % (land_id, province_id, amphur_id))
    raw_data = landprice_response.read()
    data = raw_data.decode('TIS-620') # Thai Language Characters Set
    parser = DataParser()
    parser.feed(data)
    landprice_info = parser.output[1:].split(';')
    if len(landprice_info) > 14:
        unwanted_cols = len(landprice_info) - 14
        landprice_info = landprice_info[0:12] + landprice_info[12 + unwanted_cols:]
    return landprice_info
    
    

def get_ns3_data(ns3a_no, rawang_no, province_no):
    # Extract NS3A Id
    ns3a_request_data = {
                            'ns3a_no': ns3a_no,
                            'rawang_landno': rawang_no,
                            'selChangwat': province_no
                        }
    ns3a_request_body = parse.urlencode(ns3a_request_data).encode()
    ns3a_request = request.Request(ns3_url, ns3a_request_body)
    ns3a_response = request.urlopen(ns3a_request)
    ns3a_params = re.search('NS3AReport\(\d+,\S+\)', str(ns3a_response.read())).group(0)
    params = re.search('\(\S+\)', ns3a_params).group(0)[1:-2]
    params = [int(re.search('\d+', id).group(0)) for id in params.split(',')]
    ns3aid = params[0]
    
    # Scrape NS3A data
    ns3a_response = request.urlopen(ns3data_url % ns3aid)
    raw_data = ns3a_response.read()
    data = raw_data.decode('TIS-620')
    parser = DataParser()
    parser.feed(data)
    ns3a_info = parser.output[1:].split(';')
    if len(ns3a_info) > 13:
        unwanted_cols = len(ns3a_info) - 13
        ns3a_info = ns3a_info[0:12] + ns3a_info[12 + unwanted_cols:]
    return ns3a_info

def is_excluded(data):
    for ex in excluded:
        if data.startswith(ex): 
            return True
    return False

provinces = pd.read_excel('./province.xlsx')
input_files = os.listdir('./input')
land_output = open(f'./output/land_output.csv', 'w')
ns3a_output = open(f'./output/ns3_output.csv', 'w')
land_output.write(';'.join(land_column_names))
land_output.write('\n')
ns3a_output.write(';'.join(ns3a_column_names))
ns3a_output.write('\n')

for file in input_files:
    if not file.endswith('.xlsx'): continue
    lands = pd.read_excel(f'./input/{file}')
    deeds = lands[lands['Type of Land right'] == 79]
    deeds = pd.merge(deeds, provinces, on='Province')[['Title Deed No..1', 'หน้าสำรวจ', 'Code']]
    
    print(f'Processing {file}')
    print('Get Land data')
    for i in deeds.index:
        try:
            land_no, survey_no, province_no = deeds.loc[i, 'Title Deed No..1'], int(deeds.loc[i, 'หน้าสำรวจ']), deeds.loc[i, 'Code']
            land_data = get_land_data(land_no, survey_no, province_no)
            land_output.write(';'.join(land_data))
            land_output.write('\n')
            print(f'Saved.')
        except:
            print(f'No data')
            
    ns3as = lands[lands['Type of Land right'] == 81]
    ns3as = pd.merge(ns3as, provinces, on='Province')[['Title Deed No..1', 'เลขที่ดิน', 'Code']]
    
    print('Get NS3A data')
    for i in ns3as.index:
        try:
            ns3a_no, rawang_no, province_no = ns3as.loc[i, 'Title Deed No..1'], int(ns3as.loc[i, 'เลขที่ดิน']), ns3as.loc[i, 'Code']
            ns3a_data = get_ns3_data(ns3a_no, rawang_no, province_no)
            ns3a_output.write(';'.join(ns3a_data))
            ns3a_output.write('\n')
            print('Saved')
        except:
            print('No data')