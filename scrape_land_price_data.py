import re
import pandas as pd

from urllib import parse, request
from html.parser import HTMLParser

land_url = 'http://property.treasury.go.th/pvmwebsite/search_data/s_land1_result.asp'
price_url = 'http://property.treasury.go.th/pvmwebsite/search_data/r_land_price.asp?landid=%d&changwat=%d&amphur=%d'
excluded = [
    '<!--',
    '.::: ระบบเว็บไซต์ให้บริการประชาชน :::. กรมธนารักษ์',
    'บัญชีราคาประเมินทุนทรัพย์ที่ดิน', 'สำนักงานที่ดินจังหวัด', 'สาขา', 'รอบบัญชี พ.ศ.', 
    'โฉนดเลขที่', 'อำเภอ', 'หน้าสำรวจ', 'ตำบล',
    'เครื่องหมายที่ดิน',
    'ระวาง', 'แผ่นที่', 'มาตราส่วน', 'เลขที่ดิน', 'โซน', 'บล็อก', 'ล็อท/หน่วย',
    'เนื้อที่(ไร่-งาน-ตร.วา)', 'ราคาประเมิน (บาท ต่อ ตร.วา)', 'บาท'
]
column_names = ['จังหวัด', 'อำเภอ', 'รอบบช.', 'โฉนด', 'อำเภอ2', 'หน้าสำรวจ', 'ตำบล', 'ระวาง', 'ระวาง2', 'แผนที่', 'มาตราส่วน', 'เลขที่ดิน', 'เนื้อที่', 'ราคา']

def is_excluded(data):
    for ex in excluded:
        if data.startswith(ex): 
            return True
    return False

class DataParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.output = ''
    
    def handle_data(self, data):
        data = data.strip('\\r\\n').strip()
        if len(data) > 0 and data and not is_excluded(data):
            self.output += ';' + str(data)

provinces = pd.read_excel('./province.xlsx')
lands = pd.read_excel('./Title Deed.xlsx')
deeds = lands[lands['Type of Land right'] == 79]
deeds = pd.merge(deeds, provinces, on='Province')[['Title Deed No..1', 'หน้าสำรวจ', 'Code']]

output = open('output.csv', 'w')

output.write(';'.join(column_names))
output.write('\n')
for i in deeds.index:
    chanode_no, survey_no, province_code = deeds.loc[i, 'Title Deed No..1'], deeds.loc[i, 'หน้าสำรวจ'], deeds.loc[i, 'Code']
    land_request_data = {
        'chanode_no': chanode_no,
        'survey_no': survey_no,
        'selChangwat': province_code
    }
    land_request_body = parse.urlencode(land_request_data).encode()
    land_request = request.Request(land_url, land_request_body)
    land_response = request.urlopen(land_request)
    try:
        landprice_data = re.search('LandReport\(\d+,\d+,\S+\)', str(land_response.read())).group(0)
        ids = re.search('\(\S+\)', landprice_data).group(0)[1:-2]
        ids = [int(re.search('\d+', id).group(0)) for id in ids.split(',')]
        land_id, province_id, amphur_id = ids[0], ids[1], ids[2]
        
        price_response = request.urlopen(price_url % (land_id, province_id, amphur_id))
        raw_data = price_response.read()
        data = raw_data.decode('TIS-620') # Thai Language Characters Set
        parser = DataParser()
        parser.feed(data)
        output.write(parser.output[1:])
        output.write('\n')
        print(f'Deed No:{chanode_no} Survey:{survey_no} Province:{province_code} is saved.')
    except Exception:
        print(f'Deed No:{chanode_no} Survey:{survey_no} Province:{province_code} has no data.')
