#業三專用,多筆貨櫃號碼以","做分隔

import aiohttp
import asyncio
import logging.handlers
import xml.etree.ElementTree as ET
from ext import NsCallApi
from ConnToSQL import fetch_data_from_mes
from ConnToSQL import fetch_data_from_oracle
from datetime import datetime
from xml.dom import minidom
import time
import os
import logging
from queue import Queue
import json
#------------------------log area---------------------------
# logger=logging.getLogger()
# logger.setLevel(logging.INFO)

# console_handler = logging.StreamHandler()

# fomatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
# console_handler.setFormatter(fomatter)

# logger.addHandler(console_handler)   #解除註解可看到log
#------------------------log area---------------------------
def save_xml_to_file(xml_data, filename="output_SN.xml"):
    with open(filename, "w", encoding="utf-8") as file:
        file.write(xml_data)

def DatetimeTransSN(date_str):
    return datetime.strptime(date_str, '%Y/%m/%d %H:%M:%S').strftime('%Y-%m-%dT00:00:00')

def DatetimeTransSN2(date_str):
    return datetime.strptime(date_str, '%Y%m%d').strftime('%Y-%m-%dT00:00:00')
def DatetimeTransSN3(date_str):
    return datetime.strptime(date_str, '%Y/%m/%d').strftime('%y%m%d')

def add_custom_fieldSN(parent, script_id, value=None, field_type='ns7:StringCustomFieldRef', internal_id=None, value_type='xsd:string'):
    custom_field = ET.SubElement(parent, 'ns7:customField', {'xsi:type': field_type, 'scriptId': script_id})
    
    if internal_id:
        value_element = ET.SubElement(custom_field, 'ns7:value', {'xsi:type': value_type, 'internalId': internal_id})
    else:
        value_element = ET.SubElement(custom_field, 'ns7:value', {'xsi:type': value_type})
    
    if value:
        value_element.text = value

def generate_token_passport():
    result = NsCallApi('db_base_mes.json').verifyArithmetic()
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


async def SNsendData(SearchID, log_message, batch_size=100, num_worker=5):
    #start_time = time.perf_counter()    
    await asyncio.sleep(3)  # 用 asyncio 的 sleep 替換 time.sleep    
    PodictList = fetch_data_from_oracle(SearchID)
    item_id_cache = {POdict['SKU']: NsCallApi().internalIdApi(POdict['SKU']) for POdict in PodictList}

    #將SKU轉換成itemid並以字典的方式存放在一個變數裡,模式為{SKU:itemid},例:{'1E038-0023708': '12987'}
    #print(PodictList)
    #------------------------------------------
    PoExternalID = PodictList[1]['EXTERNALID']
    ExternalIDList = PoExternalID.split(',')
    #ExternalIDList_final = ','.join(f"'{item}'" for item in ExternalIDList)
    SNdictList = fetch_data_from_mes(ExternalIDList)
    #將貨櫃號抓出來並丟進函式抓取資料庫的資料,作為查詢條件,可縮小查詢範圍,優化速度
    # if batch_size == None:
    #     batch_size = len(SNdictList)
    #print(batch_size)
    #------------------------------------------

    #print(ExternalIDList_final)
    logging.info(f"開始 SN 資料傳輸，交貨單號: {SearchID}")
    log_message.append(f"以下為 SN 資料傳輸狀況，交貨單號: {SearchID}")

    sn_data_by_prodno = {}
    for SNdict in SNdictList:
        key = (SNdict['prodno'], SNdict['carno'], SNdict['Material'])
        
        if key not in sn_data_by_prodno:
            sn_data_by_prodno[key] = []
        sn_data_by_prodno[key].append(SNdict)
        
    unique_keys = set()
    for key in sn_data_by_prodno.keys():
        if key in unique_keys:
            logging.error(f'重複的 SN 資料: {key}')
        unique_keys.add(key)
    #print(unique_keys)
        
        

    
    queue = asyncio.Queue()
#----------------------------------------------------------------
    processed_Item = set()
    
    for POdict in PodictList:
        try:
            if ',' in POdict['EXTERNALID']:
                externalID = ExternalIDList
                #print(externalID)
                for i in range(len(externalID)):
                    if POdict['交貨單號'] == SearchID:
                        prodno = POdict['生產編號']
                        carno = externalID[i]                    
                        sku = POdict['SKU']
                        if (prodno, carno, sku) not in processed_Item:
                            processed_Item.add((prodno, carno, sku))
                            keyset = (prodno,carno,sku)
                            if (prodno, carno, sku) in sn_data_by_prodno:                            
                                sn_batch = sn_data_by_prodno[(prodno, carno, sku)]
                                for j in range(0, len(sn_batch), batch_size):
                                    current_batch = [(sn, POdict) for sn in sn_batch[j:j + batch_size]]
                                    if current_batch:                                    
                                        await queue.put((current_batch))
                            else:
                                raise KeyError(f"{unique_keys} 裡不包含 {keyset} ,請詳細確認") 
                                break       
            else:
                #externalID = POdict['EXTERNALID']
                # print(POdict['SKU'])
                # print(POdict['生產編號'])
                # print(POdict['EXTERNALID'])
                if POdict['交貨單號'] == SearchID:
                    prodno = POdict['生產編號']
                    carno = POdict['EXTERNALID']                
                    sku = POdict['SKU']
                    keyset = (prodno,carno,sku)
                    #print(keyset)
                    if (prodno, carno, sku) not in processed_Item:
                        processed_Item.add((prodno, carno, sku))
                        #print(processed_Item)
                        if (prodno, carno, sku) in sn_data_by_prodno:
                            #print(processed_Item)
                            #print(unique_keys)
                            sn_batch = sn_data_by_prodno[(prodno, carno, sku)]
                            
                            for j in range(0, len(sn_batch), batch_size):
                                current_batch = [(sn, POdict) for sn in sn_batch[j:j + batch_size]]
                                if current_batch:
                                    await queue.put((current_batch))
                        else:
                            #print(f"{unique_keys} 裡不包含 {keyset} ,請詳細確認")
                            raise KeyError(f"{unique_keys} 裡不包含 {keyset} ,請詳細確認")
                            break
        except KeyError as e:
            logging.error(f"KeyError: {e}")
            log_message.append(f"KeyError: {e}")
#以上將資料每份100筆,分割成幾個小包裹,並放進排程裡
#----------------------------------------------------------------
    
    # 使用異步協程處理
    tasks = []
    #logging.info("進入worker階段")
    for _ in range(num_worker):
        task = asyncio.create_task(worker(queue,item_id_cache, log_message))
        tasks.append(task)
    #處理上述排程

    await queue.join()

    for task in tasks:
        await task

    logging.info(f"交貨單號:{SearchID} 的所有MES資料已經發送完成")
    log_message.append(f"交貨單號:{SearchID} 的所有MES資料已經發送完成")
    # end_time = time.perf_counter()
    # logging.info(f"SNsendData執行耗時: {end_time - start_time:.2f} 秒")

async def worker(queue,item_id_cache, log_message):
    #start_time = time.perf_counter()
    while not queue.empty():
        current_batch = await queue.get()
        try:
            #logging.info("進入create_and_send_xml階段")
            await create_and_send_xml(current_batch,item_id_cache, log_message)
        finally:
            queue.task_done()
    # end_time = time.perf_counter()
    # logging.info(f"worker執行耗時: {end_time - start_time:.2f} 秒")


async def create_and_send_xml(batch_PodictList,item_id_cache, log_message):
    #start_time = time.perf_counter()    
    envelope = ET.Element('soapenv:Envelope', {
        'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
        'xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance',
        'xmlns:soapenv': 'http://schemas.xmlsoap.org/soap/envelope/',
        'xmlns:platformCore': 'urn:core_2021_2.platform.webservices.netsuite.com',
        'xmlns:platformMsgs': 'urn:messages_2021_2.platform.webservices.netsuite.com',
        'xmlns:listAcct': 'urn:accounting_2021_2.lists.webservices.netsuite.com',
        'xmlns:platformCommon': 'urn:common_2021_2.platform.webservices.netsuite.com'
    })
    header = ET.SubElement(envelope, 'soapenv:Header')
    token_passport=generate_token_passport()
    header.append(token_passport)
    #logging.info("token_passport產生完成")

    body = ET.SubElement(envelope, 'soapenv:Body')
    upsert_list = ET.SubElement(body, 'upsertList', {'xmlns': 'urn:messages_2021_2.platform.webservices.netsuite.com'})
    #end_time = time.perf_counter()
    #start_time = time.perf_counter()
    #logging.info("開始產生xml")
    for SNdict,POdict in batch_PodictList:
        #print(POdict)
        ItemID=item_id_cache[SNdict['Material']]
        #ItemID='7317'          
        record = ET.SubElement(upsert_list, 'record', {
            'xmlns:ns6': 'urn:customization_2021_2.setup.webservices.netsuite.com',
            'xsi:type': 'ns6:CustomRecord'
        })
        ns6_recType = ET.SubElement(record, 'ns6:recType', {
            'xmlns:ns7': 'urn:core_2021_2.platform.webservices.netsuite.com',
            'scriptId': 'customrecord_ps_sn_entity',
            'type': 'customRecordType',
            'xsi:type': 'ns7:CustomizationRef'
        })
        ns6_externalId =ET.SubElement(record, 'ns6:externalId', {
            'xsi:type': 'xsd:string'}).text = 'TX'+SNdict['FRAMENO']

        custom_field_list = ET.SubElement(record, 'ns6:customFieldList', {'xmlns:ns7':'urn:core_2021_2.platform.webservices.netsuite.com','xsi:type':'ns7:CustomFieldList'})
        try:
            if ',' in POdict['EXTERNALID']:
                externalIDlist = POdict['EXTERNALID'].split(",")
                for externalID in externalIDlist:
                    if SNdict['Material'] == POdict['SKU']:
                        if SNdict['carno'] == externalID: 
                            if SNdict['prodno'] == POdict['生產編號']:
                                add_custom_fieldSN(custom_field_list,'custrecord_ps_cn_vendor_name',SNdict['Entity'],'ns7:StringCustomFieldRef',None)
                                add_custom_fieldSN(custom_field_list,'custrecord_ps_cn_date',DatetimeTransSN2(SNdict['cardate']),'ns7:DateCustomFieldRef',None,'xsd:dateTime')
                                if ItemID is not None:
                                    add_custom_fieldSN(custom_field_list,'custrecord_ps_cn_item',SNdict['Material'],'ns7:SelectCustomFieldRef',ItemID,'ns7:ListOrRecordRef')
                                #add_custom_fieldSN(custom_field_list,'custrecord_ps_cn_item',SNdict['Material'],'ns7:SelectCustomFieldRef','802','ns7:ListOrRecordRef')
                                add_custom_fieldSN(custom_field_list,'custrecord_ps_cn_sn',SNdict['FRAMENO'],'ns7:StringCustomFieldRef',None)
                                add_custom_fieldSN(custom_field_list,'custrecord_ps_cn_sn_cnorder',SNdict['PONO'],'ns7:StringCustomFieldRef',None)
                                add_custom_fieldSN(custom_field_list,'custrecord_ps_cn_location',POdict['LOCATIONID'],'ns7:SelectCustomFieldRef',POdict['LOCATIONID'],'ns7:ListOrRecordRef')
                                #add_custom_fieldSN(custom_field_list,'custrecord_ps_cn_location',POdict['LOCATIONID'],'ns7:SelectCustomFieldRef','9','ns7:ListOrRecordRef')
                                add_custom_fieldSN(custom_field_list,'custrecord_ps_sn_cnsn',SNdict['carno'],'ns7:StringCustomFieldRef',None)
                                add_custom_fieldSN(custom_field_list,'custrecord_ps_cn_us_po','PO'+externalID+DatetimeTransSN3(POdict['TRANDATE']),'ns7:StringCustomFieldRef',None)
                                add_custom_fieldSN(custom_field_list,'custrecord_ps_controller_no',SNdict['FWheel'],'ns7:StringCustomFieldRef',None)
                                add_custom_fieldSN(custom_field_list,'custrecord_ps_display_no',SNdict['monitor'],'ns7:StringCustomFieldRef',None)
                                add_custom_fieldSN(custom_field_list,'custrecord_ps_battery_no',SNdict['Battery'],'ns7:StringCustomFieldRef',None)
                                add_custom_fieldSN(custom_field_list,'custrecord_ps_hub_motor',SNdict['Motor'],'ns7:StringCustomFieldRef',None)
                                add_custom_fieldSN(custom_field_list,'custrecord_ps_tsensor_no',SNdict['BWheel'],'ns7:StringCustomFieldRef',None)
                                add_custom_fieldSN(custom_field_list,'custrecord_ps_assembly_date',DatetimeTransSN(SNdict['PrintDateTime']),'ns7:DateCustomFieldRef',None,'xsd:dateTime')
                                add_custom_fieldSN(custom_field_list,'custrecord_ps_qc_date',DatetimeTransSN2(SNdict['cardate']),'ns7:DateCustomFieldRef',None,'xsd:dateTime'
                                )
                                add_custom_fieldSN(custom_field_list,'custrecord_ps_bol_date',DatetimeTransSN2(SNdict['cardate']),'ns7:DateCustomFieldRef',None,'xsd:dateTime')
            else:
                externalID = POdict['EXTERNALID']
                #print(POdict['SKU'])
                if SNdict['Material'] == POdict['SKU']:                        
                    if SNdict['carno'] == POdict['EXTERNALID']: 
                        if SNdict['prodno'] == POdict['生產編號']:
                            add_custom_fieldSN(custom_field_list,'custrecord_ps_cn_vendor_name',SNdict['Entity'],'ns7:StringCustomFieldRef',None)
                            add_custom_fieldSN(custom_field_list,'custrecord_ps_cn_date',DatetimeTransSN2(SNdict['cardate']),'ns7:DateCustomFieldRef',None,'xsd:dateTime')
                            if ItemID is not None:
                                add_custom_fieldSN(custom_field_list,'custrecord_ps_cn_item',SNdict['Material'],'ns7:SelectCustomFieldRef',ItemID,'ns7:ListOrRecordRef')
                            #add_custom_fieldSN(custom_field_list,'custrecord_ps_cn_item',SNdict['Material'],'ns7:SelectCustomFieldRef','802','ns7:ListOrRecordRef')
                            add_custom_fieldSN(custom_field_list,'custrecord_ps_cn_sn',SNdict['FRAMENO'],'ns7:StringCustomFieldRef',None)
                            add_custom_fieldSN(custom_field_list,'custrecord_ps_cn_sn_cnorder',SNdict['PONO'],'ns7:StringCustomFieldRef',None)
                            add_custom_fieldSN(custom_field_list,'custrecord_ps_cn_location',POdict['LOCATIONID'],'ns7:SelectCustomFieldRef',POdict['LOCATIONID'],'ns7:ListOrRecordRef')
                            #add_custom_fieldSN(custom_field_list,'custrecord_ps_cn_location',POdict['LOCATIONID'],'ns7:SelectCustomFieldRef','9','ns7:ListOrRecordRef')
                            add_custom_fieldSN(custom_field_list,'custrecord_ps_sn_cnsn',SNdict['carno'],'ns7:StringCustomFieldRef',None)
                            add_custom_fieldSN(custom_field_list,'custrecord_ps_cn_us_po','PO'+externalID+DatetimeTransSN3(POdict['TRANDATE']),'ns7:StringCustomFieldRef',None)
                            add_custom_fieldSN(custom_field_list,'custrecord_ps_controller_no',SNdict['FWheel'],'ns7:StringCustomFieldRef',None)
                            add_custom_fieldSN(custom_field_list,'custrecord_ps_display_no',SNdict['monitor'],'ns7:StringCustomFieldRef',None)
                            add_custom_fieldSN(custom_field_list,'custrecord_ps_battery_no',SNdict['Battery'],'ns7:StringCustomFieldRef',None)
                            add_custom_fieldSN(custom_field_list,'custrecord_ps_hub_motor',SNdict['Motor'],'ns7:StringCustomFieldRef',None)
                            add_custom_fieldSN(custom_field_list,'custrecord_ps_tsensor_no',SNdict['BWheel'],'ns7:StringCustomFieldRef',None)
                            add_custom_fieldSN(custom_field_list,'custrecord_ps_assembly_date',DatetimeTransSN(SNdict['PrintDateTime']),'ns7:DateCustomFieldRef',None,'xsd:dateTime')
                            add_custom_fieldSN(custom_field_list,'custrecord_ps_qc_date',DatetimeTransSN2(SNdict['cardate']),'ns7:DateCustomFieldRef',None,'xsd:dateTime'
                            )
                            add_custom_fieldSN(custom_field_list,'custrecord_ps_bol_date',DatetimeTransSN2(SNdict['cardate']),'ns7:DateCustomFieldRef',None,'xsd:dateTime')
        except KeyError as k:
            logging.error(f'key缺失,請檢查{k}是否存在')
            log_message.append(f'key缺失,請檢查{k}是否存在')
        except Exception as e:
            logging.error(f'發生{e}錯誤,請檢查貨櫃號、生產編號及SKU是否是同一張訂單')
            log_message.append(f'發生{e}錯誤,請檢查貨櫃號、生產編號及SKU是否是同一張訂單')
    # end_time = time.perf_counter()
    # logging.info(f"截至xml產生結束,執行耗時: {end_time - start_time:.2f} 秒")
    # Generate the token passport and body

    xml_data = ET.tostring(envelope, encoding='utf-8', method='xml').decode()
    xml_dom = minidom.parseString(xml_data)
    pretty_xml_as_string = xml_dom.toprettyxml(indent="  ")
    #print(pretty_xml_as_string)
    #save_xml_to_file(pretty_xml_as_string)
    #logging.info("開始傳輸")
    await send_request(xml_data, log_message,POdict=POdict,SNdict=SNdict)


async def send_request(xml_data,  log_message, POdict, SNdict, max_retries=5, retry_delay=10):
    with open("db_base_mes.json", "r", encoding="utf-8") as file: 
        data = json.load(file)
    #logging.info("傳輸中")
    #start_time = time.perf_counter()
    url = data['url']
    headers = {
        'Content-Type': 'text/xml',
        'SOAPAction': 'upsertList'
    }
    namespaces = {
        'platformCore': 'urn:core_2021_2.platform.webservices.netsuite.com',
        'platformMsgs': 'urn:messages_2021_2.platform.webservices.netsuite.com',
        'soapenv': 'http://schemas.xmlsoap.org/soap/envelope/',
        'xsd': 'http://www.w3.org/2001/XMLSchema',
        'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
        'ns0': 'urn:messages_2021_2.platform.webservices.netsuite.com',
        'platformFaults': 'urn:faults_2021_2.platform.webservices.netsuite.com',
    }

    attempt = 0
    while attempt < max_retries:
        attempt += 1
        logging.info(f"貨櫃號碼:{SNdict['carno']},生產編號:{SNdict['prodno']},SKU:{SNdict['Material']}的第 {attempt} 次嘗試發送資料...")

        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=xml_data, headers=headers) as response:
                root = ET.fromstring(await response.text())
                elem_sent_status = root.find('.//platformCore:status', namespaces)
                elem_sent_mes = root.findall('.//ns0:baseRef', namespaces)
                faultstring = root.find('.//platformFaults:message', namespaces)

                error_message = faultstring.text if faultstring is not None else "未知錯誤"

                if faultstring is not None and "SuiteTalk concurrent request limit exceeded" in error_message:
                    logging.warning("發生限額超過錯誤，等待後重試...")
                    log_message.append(f"第 {attempt} 次發送失敗: {error_message}，等待 {retry_delay} 秒後重試")
                    await asyncio.sleep(retry_delay)  # 用 asyncio.sleep 代替 time.sleep
                    result = NsCallApi('db_base_mes.json').verifyArithmetic()
                    continue  # 重新嘗試

                elif response.status == 200:
                    logging.info(f'貨櫃號碼:{SNdict["carno"]},生產編號:{SNdict["prodno"]},SKU:{SNdict["Material"]} 的數據發送成功! , Code:{response.status}')
                    log_message.append(f'貨櫃號碼:{SNdict["carno"]},生產編號:{SNdict["prodno"]},SKU:{SNdict["Material"]} 的數據發送成功! , Code:{response.status}')
                    break
                else:
                    logging.error(f'發送失敗... , Code: {response.status}')
                    log_message.append(f'發送失敗... , Code: {response.status}')
                    print(await response.text())
                    break

    if attempt == max_retries:
        logging.error("已達到最大重試次數，停止嘗試")
        log_message.append("已達到最大重試次數，停止嘗試")
    # end_time = time.perf_counter()
    # logging.info("傳輸結束")
    # print(f"send_request執行耗時: {end_time - start_time:.2f} 秒")
if __name__ == '__main__':
    # 使用 asyncio.run() 來運行異步函數
    asyncio.run(SNsendData('1477747', []))
