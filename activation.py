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


trade = system_trading(end_date='20241120') # end_date는 예시로 20241120까지 데이터를 가져옴
start_time = datetime.now()
start_time_str = start_time.strftime("%Y%m%d %H:%M:%S")
print("시작 시간 : ", start_time_str)
# defualt setting
## 아래는 MSCI 편입 종목 중 구성 비중이 0.3% 이상인 종목들
# ['삼성전자', 'SK하이닉스', '삼성전자우', 'KB금융', '현대차', '셀트리온', 'NAVER', '신한지주', '기아', 'POSCO홀딩스',
# 'LG에너지솔루션', '알테오젠', '하나금융지주', '삼성바이오로직스', '현대모비스', 'LG화학', '삼성SDI', '한화에어로스페이스',
# 'KT&G', '카카오', '삼성물산', '삼성화재', '우리금융지주', '두산에너지빌리티', '메리츠금융지주', 'LG전자', '고려아연',
# 'HD현대일렉트릭', '크래프톤', 'HD한국조선해양', '삼성중공업', '삼성생명', 'HLB', 'SK스퀘어', 'LG', '유한양행',
# '에코프로', '삼성전기', '에코프로비엠', 'SK이노베이션', '삼성에스디에스', '한국전력', '현대차2우B', '포스코퓨처엠', SK',
# '한국항공우주', 'HD현대중공업', 'HMM', '하이브', 'DB손해보험', '대한항공', '현대글로비스', '기업은행', '한화오션', '한미반도체',
# '코웨이', '현대차우', '카카오뱅크', 'HD현대', 'SK바이오팜', '아모레퍼시픽']

msci_code = [
    '005930', '000660', '005935', '105560', '005380', '068270', '035420', '055550', '000270', '005490',
    '373220', '196170', '086790', '207940', '012330', '051910', '006400', '012450', '033780', '035720',
    '028260', '000810', '316140', '034020', '138040', '066570', '010130', '267260', '259960', '009540',
    '010140', '032830', '028300', '402340', '003550', '000100', '086520', '009150', '247540', '096770',
    '018260', '015760', '005387', '003670', '034730', '047810', '329180', '011200', '352820', '005830',
    '003490', '086280', '024110', '042660', '042700', '021240', '005385', '323410', '267250', '326030',
    '090430'
]

# kospi_code = trade.GetCodeListByMarket(market=0)
# kosdaq_code = trade.GetCodeListByMarket(market=10)
# code = trade.filtered_code(kospi_code+kosdaq_code)[:-1]
code = msci_code[:50]
# code = ['255440', '278470', '007110', '099320', '137400', '003230', '112040', '058610', '020150', '211270', '112610',
#         '011790', '039130']
# print(f"Code 갯수 : {len(code)}")
# print("Code list : ", code)
localhost = '127.0.0.1'
username = 'root'
password = '0000'
# database = 'all_ticker'
database_name = 'msci'

# class setting
# trade = system_trading(end_date="20241114")
database = manage_db(localhost, username, password, database_name)

for c in tqdm(code[1:]):
    print(f"Code : {trade.GetMasterCodeName(c)}")
    trade.rq_min_chart_data(c, '1', '0')
    # if trade.end_date is None: # 굳이 이렇게 할 필요가 있나...?
    #     df_min_data = pd.DataFrame(trade.min_data, columns=['date', 'open', 'high', 'low', 'close', 'volume', 'trade_volume'])
    # else:
    #     df_min_data = trade.min_data
    df_min_data = pd.DataFrame(trade.min_data, columns=['date', 'open', 'high', 'low', 'close', 'volume', 'trade_volume']).iloc[:-1]
    # int 32로 변경
    int_columns = df_min_data.select_dtypes(include=['int64']).columns
    df_min_data[int_columns] = df_min_data[int_columns].astype(np.int32)

    float_columns = df_min_data.select_dtypes(include=['float64']).columns
    df_min_data[float_columns] = df_min_data[float_columns].astype(np.float32)
    print(f"Input DataFrame")
    print(df_min_data, end='\n\n')

    print(f"Memory usage : {df_min_data.memory_usage().sum() / 1024 ** 2:.2f} MB")
    # 아래 주석 코드는 csv 파일을 생성하기 원할 때 활성화하면 됨
    # df_min_data.to_csv(f'{trade.GetMasterCodeName(c)}_{c}.csv', index=False)

    database.insert_data(f"{trade.GetMasterCodeName(c).lower()}_{c}", dataframe=df_min_data, unique_col='date')
    # 참조 해제 - 메모리 효율성을 위함
    del df_min_data
    gc.collect() # 가비지 컬렉션 강제 실행
    
    request_term = random.uniform(7, 10)
    print(f"Request term : {request_term} & Request count : {trade.count}")
    time.sleep(request_term)

# trade.rq_min_chart_data(code, '1', '0')
# df_min_data = pd.DataFrame(trade.min_data, columns=['date', 'open', 'high', 'low', 'close', 'volume', 'trade_volume'])
# 아래 주석 코드는 csv 파일을 생성하기 원할 때 활성화하면 됨
# df_min_data.to_csv(f'{trade.GetMasterCodeName(code)}_{code}.csv', index=False)
# database.insert_data(f'{trade.GetMasterCodeName(code)}_{code}', dataframe=df_min_data, unique_col='date')
print("코드 소요 시간")
print(datetime.now() - start_time)