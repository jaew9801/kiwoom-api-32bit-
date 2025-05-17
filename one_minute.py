import sys
from PyQt5.QtWidgets import *
from PyQt5.QAxContainer import *
from PyQt5.QtCore import *
import time
# import datetime
from datetime import datetime
import pandas as pd

request_term = 1

class system_trading():
    def __init__(self, end_date=None): # default_value = None, end_date : str
        self.ocx = QAxWidget("KHOPENAPI.KHOpenAPICtrl.1")
        print("연결했습니다.")
        self.count = 0
        self.min_data = {'date': [], 'open': [], 'high': [], 'low': [], 'close': [], 'volume': [], 'trade_volume': []}
        self.end_date = pd.to_datetime(end_date, format="%Y%m%d")

        # self.end_date = end_date
        # self.start_date = start_date

        self.ocx.OnEventConnect.connect(self.OnEventConnect)
        self.ocx.OnReceiveTrData.connect(self.OnRecieveTrData)


        self.ocx.dynamicCall("CommConnect()")
        self.login_event_loop = QEventLoop()
        self.login_event_loop.exec_()

    def rq_min_chart_data(self, itemcode, tic, justify):
        # SetInputValue로 조회하고자 하는 정보(KOA Studio에서 원하는 TR 목록에 나와있다.)를 입력
        self.ocx.dynamicCall("SetInputValue(QString, QString)", "종목코드", itemcode)
        self.ocx.dynamicCall("SetInputValue(QString, QString)", "틱범위", tic)
        self.ocx.dynamicCall("SetInputValue(QString, QString)", "수정주가구분", justify)
        # CommRqData로 SetInputValue에 입력한 정보를 서버로 전송
        self.ocx.dynamicCall("CommRqData(QString, QString, int, QString)", "opt10080_req", "opt10080", 0, "2000")
        self.tr_event_loop = QEventLoop()
        self.tr_event_loop.exec_()
        # print("Second self.remained_data boolean is ", self.remained_data)
        time.sleep(request_term) # __main__ 내장변수를 반복문 실행 시에 요청 제한을 피하기 위해 사용

        while self.remained_data == True:
            # print("While, self.remained_data boolean is ", self.remained_data, end='\n\n')
            self.ocx.dynamicCall("SetInputValue(QString, QString)", "종목코드", itemcode)
            self.ocx.dynamicCall("SetInputValue(QString, QString)", "틱범위", tic)
            self.ocx.dynamicCall("SetInputValue(QString, QString)", "수정주가구분", justify)
            self.ocx.dynamicCall("CommRqData(QString, QString, int, QString)", "opt10080_req", "opt10080", 2, "2000")
            self.tr_event_loop = QEventLoop()
            self.tr_event_loop.exec_()
            time.sleep(request_term)

    def OnEventConnect(self, err_code):
        if err_code == 0:
            print("로그인 성공")
        else:
            print("로그인 실패")
        self.login_event_loop.exit()

    # OnReceiveTrData : CommRqData로 서버에 데이터를 요청하면 서버에서 데이터를 받고,
    # 일단 요청한 데이터를 준비한 후 현 진행 상황에 대한 정보 반환
    def OnRecieveTrData(self, screen_no, rqname, trcode, record_name, next, unused1, unused2, unused3, unused4):
        # print("screen_no : ", screen_no)
        # print("rqname : ", rqname)
        # print("trcode : ", trcode)
        # print("record_name : ", record_name)
        # print("next : ", next) # str type

        if next == '2':
            # print(f"Enterance of next == 2 : {next}")
            self.remained_data = True
        elif next != '2':
            # print(f"Enterance of next != 2 : {next}")
            self.remained_data = False

        if rqname == 'opt10080_req':
            # print(f'rqname : {rqname}으로 진입했습니다.')
            self.opt10080(trcode, record_name)

        try:
            self.tr_event_loop.exit()
        except AttributeError:
            pass

    def opt10080(self, trcode, record_name):
        self.count += 1
        print(f"{self.count}'s opt10080 function is called.")
        if self.count%999 == 0:
            print("Request count is 999. Sleep 1 hour")
            time.sleep(3600)
        # opt_start_time = datetime.now()
        # print(f"opt10080 function is called at {opt_start_time}")
        getrepeatcnt = self.ocx.dynamicCall("GetRepeatCnt(QString, QString)", trcode, record_name)
        # print(f"{self.count}'s getrepeatcnt : {getrepeatcnt}")

        if self.end_date == None:
            for i in range(getrepeatcnt):
                self.collect_data(trcode, record_name, i)
                # print(f"1 unit opt10080 function have a ", datetime.now() - opt_start_time, end='\n\n\n')
        else:
            for i in range(getrepeatcnt):
                self.collect_data(trcode, record_name, i)
                if self.min_data['date'][-1] < self.end_date:
                    print(f"지정한 {self.end_date}에 도달하여, 데이터 수집을 종료합니다.")
                    # print(f"dataframe type : {type(self.min_data)}")
                    # self.min_data = pd.DataFrame(self.min_data, columns=['date', 'open', 'high', 'low', 'close', 'volume', 'trade_volume'])
                    # self.min_data = self.min_data.iloc[:-1]
                    # print("DataFrame")
                    # print(self.min_data)
                    self.remained_data = False
                    self.tr_event_loop.exit()
                    return self.min_data
            # print(f"1 unit opt10080 function have a ", datetime.now() - opt_start_time, end='\n\n\n')


    def collect_data(self, trcode, record_name, index):
        """
        서버에서 받은 데이터를 수집하는 함수
        :param trcode: TR 코드 (str)
        :param record_name: 레코드 이름 (str)
        :param index: 반복 횟수 인덱스 (int)
        """
        item_code = self.GetCommData(trcode, record_name, 0, "종목코드").strip()
        close = int(self.GetCommData(trcode, record_name, index, "현재가").strip().replace("-", ""))
        volume = int(self.GetCommData(trcode, record_name, index, "거래량").strip().replace("-", ""))
        date = self.GetCommData(trcode, record_name, index, "체결시간").strip()
        date = pd.to_datetime(date, format="%Y%m%d%H%M%S")
        open_price = int(self.GetCommData(trcode, record_name, index, "시가").strip().replace("-", ""))
        high = int(self.GetCommData(trcode, record_name, index, "고가").strip().replace("-", ""))
        low = int(self.GetCommData(trcode, record_name, index, "저가").strip().replace("-", ""))
        trade_volume = volume * close

        self.min_data['date'].append(date)
        self.min_data['open'].append(open_price)
        self.min_data['high'].append(high)
        self.min_data['low'].append(low)
        self.min_data['close'].append(close)
        self.min_data['volume'].append(volume)
        self.min_data['trade_volume'].append(trade_volume)

    def GetCommData(self, trcode, record_name, index, item_name):
        #
        data = self.ocx.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, record_name, index, item_name)
        return data

    # Request code list
    def GetCodeListByMarket(self, market): # market : 0(코스피), 10(코스닥), 8(ETF)
        code_list = self.ocx.dynamicCall("GetCodeListByMarket(QString)", market)
        code_list = code_list.split(';')
        return code_list

    # 종목코드 한글명 변환
    def GetMasterCodeName(self, code):
        code_name = self.ocx.dynamicCall("GetMasterCodeName(QString)", code)
        return code_name

    def filtered_code(self, code_list):
        code_return = []
        for code in code_list:
            code_name = self.GetMasterCodeName(code)
            if "스펙" in code_name or "4호" in code_name:
                pass # 스펙 상장
            elif "ETN" in code_name:
                pass # ETN
            elif (code_name[-1:] == "우" or "3우C" in code_name or "우B" in code_name
                  or "G3우" in code_name or "4우" in code_name):
                pass # 우선주
            else:
                code_return.append(code)
        return code_return

if __name__ == "__main__":
    start_time = datetime.now()
    print('코드 시작 시간 :', start_time)
    code = "005930"
    app = QApplication(sys.argv)
    trade = system_trading(end_date="20241120")
    trade.rq_min_chart_data(code, 1, 1)
    df_min_data = pd.DataFrame(trade.min_data, columns=['date', 'open', 'high', 'low', 'close', 'volume', 'trade_volume'])
    df_min_data.to_csv(f'{code}.csv', index=False)
    print(f"코드 소요 시간")
    print(datetime.now() - start_time)

