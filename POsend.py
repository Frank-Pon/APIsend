#業三專用,多筆貨櫃號碼以","做分隔

import aiohttp
import asyncio
import xml.etree.ElementTree as ET
from ext import NsCallApi
from ConnToSQL import fetch_data_from_mes_group
from ConnToSQL import fetch_data_from_oracle
from datetime import datetime
from xml.dom import minidom
import logging
import json
import time


# logger=logging.getLogger()
# logger.setLevel(logging.INFO)

# console_handler = logging.StreamHandler()

# fomatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
# console_handler.setFormatter(fomatter)

# logger.addHandler(console_handler)

def save_xml_to_file(xml_data, filename="output_Po.xml"):
    with open(filename, "w", encoding="utf-8") as file:
        file.write(xml_data)

def add_custom_fieldPo(parent, script_id, value, field_type='platformCore:StringCustomFieldRef'):
    custom_field = ET.SubElement(parent, 'platformCore:customField', {'xsi:type': field_type, 'scriptId': script_id})
    if value:
        ET.SubElement(custom_field, 'platformCore:value', {'xsi:type': 'xsd:string'}).text = value
    else:
        ET.SubElement(custom_field, 'platformCore:value', {'xsi:type': 'xsd:string'})

def add_itemPo(parent, internal_id, quantity, rate, expected_receipt_date):
    item = ET.SubElement(parent, 'ns8:item', {'xsi:type': 'ns8:PurchaseOrderItem'})
    ET.SubElement(item, 'ns8:item', {'xsi:type': 'platformCore:RecordRef', 'internalId': internal_id})
    ET.SubElement(item, 'ns8:quantity', {'xsi:type': 'xsd:double'}).text = str(quantity)
    if rate:
        ET.SubElement(item, 'ns8:rate', {'xsi:type': 'xsd:string'}).text = rate
    else:
        ET.SubElement(item, 'ns8:rate', {'xsi:type': 'xsd:string'})
    ET.SubElement(item, 'ns8:expectedReceiptDate', {'xsi:type': 'xsd:dateTime'}).text = expected_receipt_date

def DatetimeTransPo(date_str):
    return datetime.strptime(date_str, '%Y/%m/%d').strftime('%Y-%m-%dT00:00:00')

def DatetimeTransPo2(date_str):
    return datetime.strptime(date_str, '%Y/%m/%d').strftime('%y%m%d')

def create_result():
    result = NsCallApi('db_base.json').verifyArithmetic()
    #logging.info("result產生完成")
    token_passport = ET.Element('tokenPassport', {'xsi:type': 'platformCore:TokenPassport'})
    ET.SubElement(token_passport, 'account', {'xsi:type': 'xsd:string'}).text = result['account']
    ET.SubElement(token_passport, 'consumerKey', {'xsi:type': 'xsd:string'}).text = result['consumerKey']
    ET.SubElement(token_passport, 'token', {'xsi:type': 'xsd:string'}).text = result['tokenId']
    ET.SubElement(token_passport, 'nonce', {'xsi:type': 'xsd:string'}).text = result['nonce']  # 確保唯一性
    ET.SubElement(token_passport, 'timestamp', {'xsi:type': 'xsd:long'}).text = result['timestamp']
    ET.SubElement(token_passport, 'signature', {
        'algorithm': 'HMAC_SHA256',
        'xsi:type': 'platformCore:TokenPassportSignature'
    }).text = result['signature']  # 簽名生成函數
    return token_passport

def envelope_create():
    # 建立根節點
    envelope = ET.Element('soapenv:Envelope', {
        'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
        'xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance',
        'xmlns:soapenv': 'http://schemas.xmlsoap.org/soap/envelope/',
        'xmlns:platformCore': 'urn:core_2021_2.platform.webservices.netsuite.com',
        'xmlns:platformMsgs': 'urn:messages_2021_2.platform.webservices.netsuite.com'
    })

    # 建立 Header 節點
    header = ET.SubElement(envelope, 'soapenv:Header')
    token_passport=create_result()
    header.append(token_passport)
    return envelope

def fill_detail(Podict, record, externalID):
     # 填寫訂單相關的欄位
    ET.SubElement(record, 'ns8:customForm', {'xsi:type': 'platformCore:RecordRef', 'internalId': '143'}) #寫死,NS 伺服端代號
    ET.SubElement(record, 'ns8:entity', {'xsi:type': 'platformCore:RecordRef', 'internalId': '1360438'}) #YQ代號
    ET.SubElement(record, 'ns8:subsidiary', {'xsi:type': 'platformCore:RecordRef', 'internalId': '17'}) #寫死,NS 伺服端代號
    ET.SubElement(record, 'ns8:tranDate', {'xsi:type': 'xsd:dateTime'}).text = DatetimeTransPo(Podict['TRANDATE'])
    ET.SubElement(record, 'ns8:dueDate', {'xsi:type': 'xsd:dateTime'}).text = DatetimeTransPo(Podict['DUEDATE'])
    ET.SubElement(record, 'ns8:tranId', {'xsi:type': 'xsd:string'}).text = 'PO' + externalID + DatetimeTransPo2(Podict['TRANDATE'])
    ET.SubElement(record, 'ns8:supervisorApproval', {'xsi:type': 'xsd:boolean'}).text = 'True'
    ET.SubElement(record, 'ns8:memo', {'xsi:type': 'xsd:string'}).text = externalID
    #ET.SubElement(record, 'ns8:location', {'xsi:type': 'platformCore:RecordRef', 'internalId': '9'})
    ET.SubElement(record, 'ns8:location', {'xsi:type': 'platformCore:RecordRef', 'internalId': Podict['LOCATIONID']})
    
    # CustomFieldList
    custom_field_list = ET.SubElement(record, 'ns8:customFieldList', {'xsi:type': 'platformCore:CustomFieldList'})
    add_custom_fieldPo(custom_field_list, 'custbody_av_container_number', externalID)
    add_custom_fieldPo(custom_field_list, 'custbody_av_container_bol_number', '')
    add_custom_fieldPo(custom_field_list, 'custbody_avecaymansourcedata', 'YQ')
    add_custom_fieldPo(custom_field_list, 'custbody_ps_etd_fromtx', DatetimeTransPo(Podict['TRANDATE']), field_type='platformCore:DateCustomFieldRef')
    add_custom_fieldPo(custom_field_list, 'custbody_ps_cn_date', DatetimeTransPo(Podict['OUTDATE']), field_type='platformCore:DateCustomFieldRef')

def mes_data_finder(MesGroup,Podict,externalID,externalID_data):
    for MesGroupData in MesGroup:
        if externalID == MesGroupData['CarNo'].strip() and Podict['生產編號'].strip() == MesGroupData['ProdNo'].strip() and Podict['SKU'].strip() == MesGroupData['Material'].strip():
            uniqueKey = (Podict['生產編號'], Podict['SKU'], str(Podict['單價']), MesGroupData['台數'])
            externalID_data[externalID]['items'].add(uniqueKey)  # 添加商品資料，避免重複 

async def POsendData(SearchID, log_message):
    #start_time = time.perf_counter()
    logging.info(f"開始 PO 資料傳輸，交貨單號: {SearchID}")
    log_message.append(f"以下為 PO 資料傳輸狀況，交貨單號: {SearchID}")
    result = NsCallApi().verifyArithmetic()
    #產生token
    PodictList = fetch_data_from_oracle(SearchID)
    item_id_cache = {POdict['SKU']: NsCallApi().internalIdApi(POdict['SKU']) for POdict in PodictList}
    #將SKU的itemID先換算好
    #print(item_id_cache)
    PoExternalID = PodictList[1]['EXTERNALID']
    ExternalIDList = PoExternalID.split(',')
    #ExternalIDList_final = ','.join(f"'{item}'" for item in ExternalIDList)
    MesGroup = fetch_data_from_mes_group(ExternalIDList)

   
    uniqueKey_list=set()
    externalID_data = {}
    #ItemID='7317'
    
    for Podict in PodictList:
        #print(Podict)
        if Podict['交貨單號'] == SearchID:
            if ',' in Podict['EXTERNALID']:
                externalIDlist = Podict['EXTERNALID'].split(",")
                for externalID in externalIDlist:
                    externalID = externalID.strip()
                    if externalID not in externalID_data:
                        externalID_data[externalID] = {
                            'Podict': Podict,
                            'items': set()  # 使用 set 來避免重複的商品資料
                        }
                    mes_data_finder(MesGroup,Podict,externalID,externalID_data)
            else:
                externalID = Podict['EXTERNALID'].strip() #只有單一貨櫃號碼的話就直接指定
                if externalID not in externalID_data: #如果貨櫃號碼不在列表內,就添加
                    externalID_data[externalID] = {
                                'Podict': Podict,
                                'items': set()  # 使用 set 來避免重複的商品資料
                            }
                #print(externalID_data)
                mes_data_finder(MesGroup,Podict,externalID,externalID_data)               

    for externalID, data in externalID_data.items():
        envelope = envelope_create()
        body = ET.SubElement(envelope, 'soapenv:Body')

        add = ET.SubElement(body, 'add', {'xmlns': 'urn:messages_2021_2.platform.webservices.netsuite.com'})
        record = ET.SubElement(add, 'record', {
            'xmlns:ns8': 'urn:purchases_2021_2.transactions.webservices.netsuite.com',
            'xsi:type': 'ns8:PurchaseOrder',
            'externalId': externalID
        })

        Podict = data['Podict']
       
        fill_detail(Podict, record, externalID)
        item_list = ET.SubElement(record, 'ns8:itemList', {'xsi:type': 'ns8:PurchaseOrderItemList'})
        #print(data['items'])
        for uniqueKey in data['items']:
            生產編號, SKU, 單價, 台數 = uniqueKey
            ItemID = item_id_cache[SKU]
            #print(ItemID)
            add_itemPo(item_list, ItemID, 台數, 單價, DatetimeTransPo(Podict['DUEDATE']))

                        
        xml_data = ET.tostring(envelope, encoding='utf-8', method='xml').decode()
        xml_dom = minidom.parseString(xml_data)
        pretty_xml_as_string = xml_dom.toprettyxml(indent="  ")
        result = NsCallApi().verifyArithmetic()
        #print(pretty_xml_as_string)  # Debug
        #save_xml_to_file(pretty_xml_as_string)
        await send_po_request(xml_data,log_message, SearchID, externalID)
        await asyncio.sleep(5)
        #end_time = time.perf_counter()
        #logging.info(f"總耗時: {end_time - start_time:.2f} 秒")

 


async def send_po_request(xml_data, log_message, SearchID, externalID, max_retries=3, retry_delay=5):
    #start_time = time.perf_counter()
    with open("db_base.json","r",encoding="utf-8") as file:
        data=json.load(file)

    async with aiohttp.ClientSession() as session:
        attempt = 0
        while attempt < max_retries:
            attempt += 1
            logging.info(f"第 {attempt} 次嘗試發送交貨單號:{SearchID},貨櫃號碼:{externalID}的 PO 資料...")

            headers = {
                'Content-Type': 'text/xml',
                'SOAPAction': 'add'
            }

            async with session.post(data['url'], data=xml_data, headers=headers) as response:
                namespaces = {
                    'platformCore': 'urn:core_2021_2.platform.webservices.netsuite.com',
                    'platformMsgs': 'urn:messages_2021_2.platform.webservices.netsuite.com',
                    'soapenv': 'http://schemas.xmlsoap.org/soap/envelope/',
                    'xsd': 'http://www.w3.org/2001/XMLSchema',
                    'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
                }
                root = ET.fromstring(await response.text())
                sent_status_element = root.find('.//platformCore:status', namespaces)
                sent_mes_element = root.find('.//platformCore:message', namespaces)
                #print(await response.text())
                if sent_mes_element is not None and response.status == 200:
                    sent_status = sent_status_element.attrib.get('isSuccess')
                    sent_mes = sent_mes_element.text
                    logging.info(f"交貨單號:{SearchID},貨櫃號碼:{externalID} 的 PO 數據發送成功! , Code:{response.status}")
                    logging.info(f"伺服器接收狀況: {sent_status}")
                    logging.info(f"訊息回饋: {sent_mes}")
                    log_message.append(f"PO 數據發送成功! , Code:{response.status}")
                    log_message.append(f"伺服器接收狀況: {sent_status}")
                    log_message.append(f"訊息回饋: {sent_mes}")
                    break
                else:
                    logging.error(f"PO 數據發送失敗... , Code:{response.status}")
                    logging.error(f"訊息回饋: {await response.text()}")
                    log_message.append(f"PO 數據發送失敗... , Code:{response.status}")
                    log_message.append(f"訊息回饋: {await response.text()}")
                    #print(await response.text())
                    await asyncio.sleep(retry_delay)  # 使用 asyncio.sleep

    if attempt == max_retries:
        logging.error("已達到最大重試次數，停止嘗試")
        log_message.append("已達到最大重試次數，停止嘗試")

    #end_time = time.perf_counter()
    #logging.info(f"傳輸執行耗時: {end_time - start_time:.2f} 秒")
if __name__ == '__main__':
    # 使用 asyncio.run() 來運行異步函數
    asyncio.run(POsendData('1477747', []))