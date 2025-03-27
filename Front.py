#業三專用,多筆貨櫃號碼以","做分隔

import tornado.ioloop
import tornado.web
import tornado.gen
from tornado.httpclient import AsyncHTTPClient
from SNsend import SNsendData
from POsend import POsendData
from ConnToSQL import fetch_data_from_oracle_noOutdate, fetch_data_from_mes,fetch_data_from_mes_group
import logging
import json
from datetime import datetime
import smtplib
import asyncio
from email.mime.text import MIMEText

log_message =[]

# 設定 logging 基本配置
logging.basicConfig(filename='app.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# 如果你也想在控制台顯示日誌，可以加入一個 StreamHandler
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console.setFormatter(formatter)
logging.getLogger().addHandler(console)

def DatetimeTransPo():
    return datetime.now().strftime('%Y-%m-%d')

def DatetimeTransPo2(date_str):
    return datetime.strptime(date_str, '%Y/%m/%d').strftime('%y%m%d')





class LogStreamHandler(tornado.web.RequestHandler):
    async def get(self):
        self.set_header("Content-Type", "text/event-stream")
        self.set_header("Cache-Control", "no-cache")
        self.set_header("Connection", "keep-alive")

        log_message.append('資料傳輸中...')
        self.write(f"data: 資料傳輸中...\n\n")
        self.flush()  # 刷新緩衝區，立即推送日誌

        try:
            while True:
                # 如果已经有日志信息，逐条推送
                if self.application.logs:
                    log = self.application.logs.pop(0)
                    self.write(f"data: {log}\n\n")
                    self.flush()

                await tornado.gen.sleep(1)
                if "資料傳輸完成!" in log_message:
                    break 
            
            self.write("data:資料傳輸完成!\n\n")
            self.flush()
        except Exception as e:
            error_message = f"資料傳輸過程發生錯誤:{str(e)}"
            self.write(f"data:{error_message}\n\n")
            self.flush()
        
        self.finish()

def generate_new_logs():
    """返回新生成的日誌"""
    global log_message
    new_logs = log_message[:]  # 複製 log_message
    log_message.clear()  # 清空消息避免重複推送
    return new_logs



class homehandler(tornado.web.RequestHandler):
    async def get(self):
        log_message =[]
        oracle_data = []
        mssql_data = []
        mes_group_data=[]
        mes_group_count=0
        oracle_count = 0
        mssql_count = 0
        total = 0
        error_message = None
        tranDate = None
        NowDate = datetime.now().strftime('%Y%m%d_%H%M%S')
        mes_HTML = []
        record_HTML = []
        self.render("index.html", oracle_data=oracle_data, mssql_data=mssql_data,
                    oracle_count=oracle_count, mssql_count=mssql_count,
                    error=error_message, logs=log_message, total=total,mes_group_data=mes_group_data,tranDate=tranDate
                    ,mes_group_count=mes_group_count,mes_HTML=mes_HTML,record_HTML=record_HTML,NowDate=NowDate)

    async def post(self):    
        log_message =[]
        oracle_data = []
        mssql_data = []
        mes_group_data=[]
        mes_group_count=0
        oracle_count = 0
        mssql_count = 0
        total = 0
        error_message = None
        tranDate = None
        NowDate = datetime.now().strftime('%Y%m%d_%H%M%S')
        mes_HTML = []
        record_HTML = []
        processed_items=set()
        try:
            order_id = self.get_body_argument("orderID",None)
            message=f"已獲取交貨單號:{order_id},查詢中......"
            logging.info(message)
            log_message.append(message)
                

            oracle_data = fetch_order_from_oracle(order_id)
            oracle_count = len(oracle_data) if oracle_data!=[{"Error": "No matching Oracle records found"}] else 0 

            if oracle_count == 0:
                total = 0
                raise ValueError(f"未找到符合交貨單號:{order_id}的資料,請確認後重新嘗試")
                
            else:
                match_found = False
                for record in oracle_data:
                          # 確保正確獲取 MES 資料
                        if order_id == record['交貨單號']:
                            # if ',' in record['EXTERNALID']:
                                externalID_list = [ext.strip() for ext in record['EXTERNALID'].split(',')]
                                mes_group_data = fetch_mes_group_data(externalID_list)
                                for externalID in externalID_list:
                                    for mes in mes_group_data:                                        
                                        if externalID == mes['CarNo'] and record['生產編號'] == mes['ProdNo'] and record['SKU'] == mes['Material']:
                                            uniqueKey = (externalID, mes['ProdNo'], mes['Material'])
                                            if uniqueKey not in processed_items:
                                                processed_items.add(uniqueKey)
                                                mes_HTML.append(mes)
                                                record_HTML.append(record)
                                                match_found = True
                            # else:                                
                            #     externalID = [ext.strip() for ext in record['EXTERNALID'].split(',')]
                            #     mes_group_data = fetch_mes_group_data(externalID)
                            #     for mes in mes_group_data:
                            #         if externalID == mes['CarNo'] and record['生產編號'] == mes['ProdNo'] and record['SKU'] == mes['Material']:
                            #             uniqueKey = (externalID, mes['ProdNo'], mes['Material'])
                            #             if uniqueKey not in processed_items:
                            #                 # processed_items.add(uniqueKey)
                            #                 # mes_HTML.append(mes)
                            #                 # record_HTML.append(record)
                            #                 match_found = True
                            
            
            if not match_found:
                error_message = f"未找到交貨單號: {order_id}的資料，請確認後重新嘗試"
                logging.error(error_message)
                log_message.append(error_message)
                raise ValueError(error_message)
            else:  
                message = f"交貨單號: {order_id}, 查詢完成!"
                logging.info(message)
                log_message.append(message)
                for quality in oracle_data:
                    total+=quality['台數']

            

            for i in oracle_data:
                    tranDate = DatetimeTransPo2(i['TRANDATE'])
                    externalID_list = [ext.strip() for ext in i['EXTERNALID'].split(',')]
                    mes_group_data = fetch_mes_group_data(externalID_list)            
                    mes_data = fetch_all_mes_data(externalID_list)
                    mssql_data.extend(item for item in mes_data if item not in mssql_data)  # 累積 MSSQL 資料                   
            mssql_count = len(mssql_data)
            mes_group_count= len(mes_group_data)

            if mes_data==[{"Error": "No matching MES records found"}]:
                raise ValueError(f"未找到符合交貨單號:{i['生產編號']}的資料")
            elif mes_data:
                message=f'找到{mssql_count}筆MSSQL資料'
                logging.info(message)
                log_message.append(message)
              # MSSQL 資料總筆數
            if total != mssql_count:
                error_message = f"台數總和: {total} 與 MSSQL 資料筆數: {mssql_count} 不一致，無法進行資料傳輸"
                logging.error(error_message)
                log_message.append(error_message)

        except ValueError as e:
            error_message = str(e)
            logging.error(f"{e}")
            log_message.append(f"{e}")

        except Exception as e:
            error_message = f"系統發生錯誤，請盡快通知維修人員: {str(e)}"
            logging.exception(f"發生未預期錯誤: {e}")
            log_message.append(f"未預期錯誤: {e}")

        self.render("index.html", oracle_data=oracle_data, mssql_data=mssql_data,
                    oracle_count=oracle_count, mssql_count=mssql_count,
                    error=error_message, logs=log_message, total=total,mes_group_data=mes_group_data,tranDate=tranDate
                    ,mes_group_count=mes_group_count,mes_HTML=mes_HTML,record_HTML=record_HTML,NowDate=NowDate)
        
class SendDataHandler(tornado.web.RequestHandler):
    async def post(self):
        
        try:
            data = json.loads(self.request.body)
            orderID = data.get('orderID')
            log_message.append(f"開始資料傳輸，交貨單號：{orderID}")

            self.application.logs.clear()
            sn_success = True
            po_success = True
            try:
                # 逐步傳輸SN數據並生成日誌
                await self.log_and_send_data('', SNsendData, orderID)
            except KeyError as e:  # 捕獲 KeyError
                sn_success = False
                logging.error(f"資料傳輸失敗: KeyError - {str(e)}")
                self.application.logs.append(f"資料傳輸失敗: KeyError - {str(e)}")
            except Exception as e:
                sn_success = False
                logging.error(f"資料傳輸失敗: SN - {str(e)}")
                self.application.logs.append(f"資料傳輸失敗: SN - {str(e)}")

            try:
                if sn_success:
                    # 逐步傳輸PO數據並生成日誌
                    await self.log_and_send_data('', POsendData, orderID)
            except Exception as e:
                po_success = False
                logging.error(f"資料傳輸失敗: PO - {str(e)}")
                self.application.logs.append(f"資料傳輸失敗: PO - {str(e)}")
            
            if sn_success and po_success:
            # 傳輸完成之後發送郵件
                await self.send_email_notification(orderID)

                self.application.logs.append("資料傳輸完成!")

                self.write({"status": "success"})
                logging.info("郵件發送完成")
                self.application.logs.append("郵件發送完成")


        except Exception as e:
            error_message = f"資料傳輸失敗，原因: {str(e)}"
            self.application.logs.append(error_message)
            self.set_status(500)
            self.write({"status": "error"})
            logging.error(f"資料傳輸失敗: {str(e)}")
            self.application.logs.append(f"資料傳輸失敗: {str(e)}")


    async def send_email_notification(self, orderID):
        """發送郵件通知"""
        try:
            # 設置郵件内容
            POnum = self.POnum_get(orderID)
            subject = f"{DatetimeTransPo()} PO creation push notification"
            body = "Dear All,\nThe list of successful PO creation push is as follows:\n"
            body += "\n".join(POnum)
            body += f"\n\nThe above {len(POnum)} POs, all items have been successfully pushed."
            body += "\n\nNOTE: This is an automated reply from a system mailbox. Please do not reply to this email. Thank you"
            msg = MIMEText(body)
            msg['Subject'] = subject
            MailList = ['marco@aventon.com','lynn@aventon.com','abaw@aventon.com','mia.qu@aventon.com','oriel@aventon.com','djames@aventon.com','yancheng.mo@aventon.com','swalston@aventon.com','aventonsys@163.com','wangcg@jhfinetech.com','lusia.yang@aventon.com','lusia.yang@aventon.cc','zjiang@aventon.com','kira.jiang@aventon.com','webmaster@mingcycle.com.tw','lilytang@mingcycle.com.tw']
            #MailList = ['cry133216@gmail.com', 'webmaster@mingcycle.com.tw']
            
            msg['From'] = 'webmaster@mingcycle.com.tw'
            msg['To'] = ', '.join(MailList)  # 在這裡指定收件人
            

            # 使用 smtplib 發送郵件（包裝成非同步操作）
            logging.info(f"準備發送郵件到: {msg['To']}")
            self.application.logs.append(f"準備發送郵件到 {msg['To']}")
            await asyncio.to_thread(self.send_email, msg)
            logging.info(f"郵件發送成功，交貨單號: {orderID}")
            self.application.logs.append(f"郵件發送成功，交貨單號: {orderID}")

        except Exception as e:
            logging.error(f"發送郵件失敗，交貨單號: {orderID}，錯誤: {e}")
            self.application.logs.append(f"發送郵件失敗，交貨單號: {orderID}，錯誤: {e}")

            raise e

    def send_email(self, msg):
        """實際發送郵件的同步函數"""
        try:
            with smtplib.SMTP('mail.mingcycle.com.tw', 587) as server:
                server.starttls()
                server.login('webmaster@mingcycle.com.tw', 'W@b2930')
                server.sendmail(msg['From'], msg['To'].split(', '), msg.as_string())
            logging.info(f"郵件通知已發送至 {msg['To']}")
            self.application.logs.append(f"郵件通知已發送至 {msg['To']}")

        except Exception as e:
            logging.error(f"郵件發送失敗: {e}")
            self.application.logs.append(f"郵件發送失敗: {e}")        


    def POnum_get(self,order_id):
        processed_items=[]
        oracle_data = fetch_order_from_oracle(order_id)
        for record in oracle_data:
            if ',' in record['EXTERNALID']:
                externalID_list = [ext.strip() for ext in record['EXTERNALID'].split(',')]
                for externalID in externalID_list:
                    uniqueKey = externalID
                    if uniqueKey not in processed_items:
                        processed_items.append(uniqueKey)
            else:
                uniqueKey = record['EXTERNALID']
                if uniqueKey not in processed_items:
                        processed_items.append(uniqueKey)
        return processed_items

    


    async def log_and_send_data(self, message, send_function, orderID):
        """记录日志并调用发送数据函数"""
        self.application.logs.append(message)
        logging.info(message)

        await tornado.gen.sleep(1)  # 模拟异步操作
        await send_function(orderID, self.application.logs)



def make_app():
    app = tornado.web.Application([
        (r'/',homehandler),
        (r'/send-data',SendDataHandler),
        (r'/log-stream',LogStreamHandler),
    ])
    app.logs = []  # 给应用添加 logs 属性来存储日志
    return app

# 查詢 MSSQL 資料庫的函數
def fetch_all_mes_data(order_id):
    results = fetch_data_from_mes(order_id)
    orderid_set= set(order_id)
    matching_records = [order for order in results if order['carno'] in orderid_set]
    return matching_records if matching_records else [{"Error": "No matching MES records found"}]

def fetch_mes_group_data(order_id):
    results = fetch_data_from_mes_group(order_id)
    orderid_set= set(order_id)
    matching_records = [order for order in results if order['CarNo'] in orderid_set]
    return matching_records if matching_records else [{"Error": "No matching MES records found"}]

def DatetimeTransSN(date_str):
    return datetime.strptime(date_str, '%Y/%m/%d %H:%M:%S').strftime('%Y-%m-%d 00:00:00')

# 查詢 Oracle 資料庫的函數
def fetch_order_from_oracle(order_id):
    results = fetch_data_from_oracle_noOutdate(order_id)
    #matching_records = [i for i in results if i['交貨單號'] == order_id]
    #return matching_records if matching_records else [{"Error": "No matching Oracle records found"}]
    return results if results else [{"Error": "No matching Oracle records found"}]
    

if __name__ == "__main__":
    app = make_app()
    app.listen(5000)
    #遠端電腦的IP 使用 http://192.168.30.86:5000 來進入網頁使用
    logging.info('已啟動完成,請開始使用')  
    tornado.ioloop.IOLoop.current().start()