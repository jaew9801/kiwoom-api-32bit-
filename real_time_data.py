import sys
from PyQt5.QtWidgets import *
from PyQt5.QAxContainer import *
import datetime


class MyWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Real")
        self.setGeometry(300, 300, 300, 400)

        btn = QPushButton("Register", self)
        btn.move(20, 20)
        btn.clicked.connect(self.btn_clicked)

        btn2 = QPushButton("DisConnect", self)
        btn2.move(20, 100)
        btn2.clicked.connect(self.btn2_clicked)

        self.ocx = QAxWidget("KHOPENAPI.KHOpenAPICtrl.1")
        self.ocx.OnEventConnect.connect(self._handler_login)
        self.ocx.OnReceiveRealData.connect(self._handler_real_data)
        self.CommmConnect()

    def btn_clicked(self):
        #self.SetRealReg("1000", "005930", "20;10", 0)
        self.SetRealReg("2000", "012450", "20;214", "0")
        # 012450 : 한화에어로스페이스
        # 215 : 장운영구분, 20 : 체결시간, 214 : 장시작예상잔여시간
        print("called\n")

    def btn2_clicked(self):
        self.DisConnectRealData("2000") # screen_no
        self.close_window()

    def close_window(self):
        self.close()

    def CommmConnect(self):
        self.ocx.dynamicCall("CommConnect()")
        self.statusBar().showMessage("login 중 ...")

    def _handler_login(self, err_code):
        if err_code == 0:
            self.statusBar().showMessage("login 완료")


    def _handler_real_data(self, code, real_type, data):
        # print('code :', code, end='\n\n')
        # print('real_type :', real_type, end='\n\n')
        # print('data :', data, end='\n\n')
        if real_type == "주식체결":
            gubun = self.GetCommRealData(code, 215)
            if gubun == 0:
                print('장 시작 전')
            elif gubun == 2:
                print("장 종료 전")
            elif gubun == 3:
                print("장 시작")
            elif gubun == 4 or gubun == 8:
                print("3시30분 장 종료")
            elif gubun == 9:
                print("장 마감")
            remained_time = self.GetCommRealData(code, 214)
            print('장 시작 예상 잔여 시간 :', remained_time)


    def SetRealReg(self, screen_no, code_list, fid_list, real_type):
        self.ocx.dynamicCall("SetRealReg(QString, QString, QString, QString)",
                              screen_no, code_list, fid_list, real_type)

    def DisConnectRealData(self, screen_no):
        self.ocx.dynamicCall("DisConnectRealData(QString)", screen_no)

    def GetCommRealData(self, code, fid):
        data = self.ocx.dynamicCall("GetCommRealData(QString, int)", code, fid)
        print('GetCommRealData fid :', fid)
        return data


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MyWindow()
    window.show()
    app.exec_()
