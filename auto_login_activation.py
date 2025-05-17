import sys
from PyQt5.QtWidgets import *
from PyQt5.QAxContainer import *
from PyQt5.QtCore import *
import time
# import datetime
from datetime import datetime
import pandas as pd
import numpy as np

from tqdm import tqdm
import random
import gc

# request_term = 0.4

# 1 minute.py
from one_minute import system_trading
# database : database.py
from database import manage_db
# from database_v2 import manage_db
app = QApplication(sys.argv)

trade = system_trading()
# trade = system_trading() # end_date 미지정
start_time = datetime.now()
start_time_str = start_time.strftime("%Y%m%d %H:%M:%S")
print("시작 시간 : ", start_time_str)

msci_code = [
    '005930', '000660', '005935', '105560', '005380', '068270', '035420', '055550', '000270', '005490',
    '373220', '196170', '086790', '207940', '012330', '051910', '006400', '012450', '033780', '035720',
    '028260', '000810', '316140', '034020', '138040', '066570', '010130', '267260', '259960', '009540',
    '010140', '032830', '028300', '402340', '003550', '000100', '086520', '009150', '247540', '096770',
    '018260', '015760', '005387', '003670', '034730', '047810', '329180', '011200', '352820', '005830',
    '003490', '086280', '024110', '042660', '042700', '021240', '005385', '323410', '267250', '326030',
    '090430'
]

code = msci_code[:50]
# code =[
#     "005930", "000660", "005380", "051910", "035420",
#     "035720", "068270", "207940", "012330", "005490",
#     "105560", "055550", "066570", "096770", "006400",
#     "015760", "000270", "028260", "047050", "086790",
#     "316140", "033780", "010950", "090430", "051900"
# ]

localhost = '127.0.0.1'

username = 'root'
password = '0000'
# database = 'msci'
database = 'test'
database = manage_db(localhost, username, password, database)

iteration_count = 0

while True:
    if iteration_count != 0: # 0이 아닌데 이 과정에 들어오면 break 됐기 때문
        app = QApplication(sys.argv)
        trade = system_trading()
        request_term = random.uniform(7, 10)
        print(f"Break Request term : {request_term} & Request count : {trade.count}", end='\n\n')
        time.sleep(request_term)
    for c in tqdm(code[15+iteration_count:]):
        iteration_count += 1
        print(f"Code : {trade.GetMasterCodeName(c)}")
        trade.rq_min_chart_data(c, '1', '0')
        df_min_data = pd.DataFrame(trade.min_data,
                                   columns=['date', 'open', 'high', 'low', 'close', 'volume', 'trade_volume'])
        # int 32로 변경
        int_columns = df_min_data.select_dtypes(include=['int64']).columns
        df_min_data[int_columns] = df_min_data[int_columns].astype(np.int32)

        # float 32로 변경
        float_columns = df_min_data.select_dtypes(include=['float64']).columns
        df_min_data[float_columns] = df_min_data[float_columns].astype(np.float32)

        print(f"Memory usage : {df_min_data.memory_usage().sum() / 1024 ** 2:.2f} MB", end='\n\n')
        print(f"Request count : {trade.count}", end='\n\n')

        database.insert_data(f"{trade.GetMasterCodeName(c)}_{c}", dataframe=df_min_data, unique_col='date')
        del df_min_data
        gc.collect()
        if 950 < trade.count < 1000:
            # df_min_data = None # df_min_data 초기화하여 메모리 확보하기 위함
            print("RQ Count is over 950 & under 1000")
            trade.tr_event_loop.exit()
            break # break 대신에 api 끊고 다시 연결하는 코드 추가 필요
        request_term = random.uniform(7, 10)
        print(f"Request term : {request_term}", end='\n\n\n')
        time.sleep(request_term)

    if iteration_count == len(code):
        print(f"while loop is breaked")
        break