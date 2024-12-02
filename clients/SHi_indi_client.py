import time
import socket
from datetime import datetime

from PyQt5.QtWidgets import QApplication, QMainWindow
from PyQt5.QAxContainer import QAxWidget
from PyQt5.QtCore import QEventLoop

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Config

import json

class SHiIndiClient(QMainWindow):    
    def __init__(self, host=Config.SERVER_HOST, port=Config.SERVER_PORT):
        super().__init__()
        self.host = host
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        self.event_loop = QEventLoop()
        self.rqidD = {}
        self.IndiTR = QAxWidget("SHINHANINDI.shinhanINDICtrl.1")
        self.IndiReal = QAxWidget("SHINHANINDI.shinhanINDICtrl.1")
        self.IndiTR.ReceiveData.connect(self.ReceiveData)
        self.IndiTR.ReceiveSysMsg.connect(self.ReceiveSysMsg)
        self.IndiReal.ReceiveRTData.connect(self.ReceiveRTData)
        self.mst_names = []
        self.mst_ready = False
        self.code_list = []

    def initialize_indi(self):
        self.setWindowTitle("Market Data client")
        self.mst_names = ['fut_mst', 'cfut_mst', 'fri_mst', 'gmf_mst']  # KOSPI 선물, 옵션, 상품선물, 유렉스, 해외지수, 야간달러선물 종목코드 조회

        try:
            print("indi 초기화 시작")

            # indi login
            # if self.IndiTR.StartIndi(Config.SHi_indi_ID, Config.SHi_indi_PW, Config.SHi_indi_Auth, Config.SHi_indi_Path):
            #     self.event_loop.exec_()
            #     print("indi 로그인 성공")

            self.getCodeList()  # 종목코드 리스트 수신
            if self.mst_ready:
                print(f"종목코드 리스트 수신 성공: {self.code_list}")
            else:
                raise Exception
            
            print("indi 초기화 성공")
            return True

        except Exception as e:
            print(f"indi 초기화 중 오류 발생: {e}")
            return False
        
    def getCodeList(self):
        """종목코드 리스트 수신"""
        for name in self.mst_names:
            ret = self.IndiTR.dynamicCall("SetQueryName(QString)", name)
            if ret:
                rqid = self.IndiTR.dynamicCall("RequestData()")
                self.rqidD[rqid] = name
                self.event_loop.exec_()

    def ReceiveData(self, rqid):
        """데이터 수신시"""
        name = self.rqidD[rqid]
        print(f"종목코드 리스트 수신 중: {name}")

        if name in self.mst_names:
            cnt = self.IndiTR.dynamicCall("GetMultiRowCount()")
            if cnt > 0:
                for i in range(cnt):
                    dictMst = {}
                    if name == 'fut_mst':
                        shortCode = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 1)
                        if shortCode.startswith('10'):  # 1: 선물 / 01: 코스피200, 04: 변동성, 05: 미니선물, 06: 코스닥, 07: 유로스톡스50, 08: KRX300
                            dictMst['구분']= name
                            dictMst['단축코드'] = shortCode
                            dictMst['종목명'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 2)
                    elif name == 'cfut_mst':
                        dictMst['구분']= name
                        dictMst['단축코드'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 1)
                        dictMst['종목명'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 3)
                    elif name == 'fri_mst':
                        dictMst['구분']= name
                        dictMst['단축코드'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 2)   # 심벌
                        dictMst['종목명'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 4)
                    else:
                        dictMst['구분']= name
                        dictMst['단축코드'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 1)
                        dictMst['종목명'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 2)
                    self.code_list.append(dictMst)

            if name == self.mst_names[-1]:
                dictMst = {'구분': 'c_mst', '단축코드': 'USD00', '종목명': 'USDKRW'}
                self.code_list.append(dictMst)
                self.mst_ready = True

        self.code_list = [item for item in self.code_list if item]  # self.code_list 에서 값이 비어있는 건 제거
        self.event_loop.exit()

    def ReceiveRTData(self, RealType):
        if any([RealType == 'FC', RealType == 'MC']):
            item_code = self.IndiTR.dynamicCall("GetSingleData(int)", 1)    # 단축코드
            current_price = self.IndiTR.dynamicCall("GetSingleData(int)", 4)  # 현재가
            current_vol = self.IndiTR.dynamicCall("GetSingleData(int)", 10)  # 단위체결량
        elif any([RealType == 'MX', RealType == 'IC']):
            item_code = self.IndiTR.dynamicCall("GetSingleData(int)", 0)    # 단축코드 or 업종코드
            current_price = self.IndiTR.dynamicCall("GetSingleData(int)", 3)  # 현재가
            current_vol = self.IndiTR.dynamicCall("GetSingleData(int)", 9)  # 단위체결량
        elif RealType == 'OC':
            print("데이터 수신됨. 작업 필요")

        data = [{'item_code': item_code, 'current_price': current_price, 'current_vol': current_vol}]
        print(f"수신데이터: {data}")
        processed_data = self.process_data(data)
        self.send_to_server(processed_data)

    def ReceiveSysMsg(self, MsgCode):
        if MsgCode == '11':
            print("시스템이 시작됨")
            self.event_loop.exit()

    def request_RT_data(self):
        for code in self.code_list:
            if code['구분'] == 'fut_mst':
                tr = 'FC'
            elif code['구분'] == 'cfut_mst':
                tr = 'MC'
            elif code['구분'] == 'fri_mst':
                tr = 'MX'
            elif code['구분'] == 'gmf_mst':
                tr = 'OC'
            self.IndiReal.dynamicCall("RequestRTReg(QString, QString)", tr, code['단축코드'])

        self.IndiReal.dynamicCall("RequestRTReg(QString, QString)", 'IC', '0001')   # 종합주가지수(KOSPI)
        self.IndiReal.dynamicCall("RequestRTReg(QString, QString)", 'IC', '1001')   # 종합지수(KOSDAQ)
        self.IndiReal.dynamicCall("RequestRTReg(QString, QString)", 'IC', '1002')   # KOSDAQ 100
        self.IndiReal.dynamicCall("RequestRTReg(QString, QString)", 'IC', '2101')   # KOSPI200 종합
        # self.IndiReal.dynamicCall("RequestRTReg(QString, QString)", 'IK', '2101')   # KOSPI200 Index (추정치)

    def _determine_data_type(self, raw_data):
        if not raw_data:
            return None
        if any('current_price' in item for item in raw_data):
            return 'price'
        # 향후 다른 타입들 추가 가능
        # if any('news_content' in item for item in raw_data):
        #     return 'news'
        # if any('trading_volume' in item for item in raw_data):
        #     return 'volume'
        return None
    
    def process_data(self, raw_data):
        processed_data = {
            'source': 'SHINHANi',
            'timestamp': datetime.now().isoformat(),
            'data_type': self._determine_data_type(raw_data),
            'content': raw_data
        }
        return processed_data
        
    def send_to_server(self, data):
        try:
            # JSON 직렬화
            serialized_data = json.dumps(data).encode('utf-8')
            
            # 크기와 함께 전송
            size_bytes = len(serialized_data).to_bytes(4, byteorder='big')
            self.socket.sendall(size_bytes + serialized_data)
            return True
            
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError) as e:
            print(f"서버 연결이 끊어졌습니다: {e}")
            return False
        except Exception as e:
            print(f"전송 실패: {e}")
            import traceback
            traceback.print_exc()
            return False
        
    def connect_to_server(self):
        """서버에 연결 시도"""
        while True:
            try:
                self.socket.connect((self.host, self.port))
                print("서버 연결 성공")
                return True
            except ConnectionRefusedError:
                print("서버에 연결할 수 없습니다. 5초 후에 재시도합니다...")
                time.sleep(5)
            except Exception as e:
                print(f"연결 실패: {e}")
                return False

if __name__ == "__main__":
    app = QApplication(sys.argv)
    client = SHiIndiClient()
    try:
        # indi 초기화 먼저 수행
        if client.initialize_indi():
            while True:
                if client.connect_to_server():
                    break
                else:
                    print("서버에 연결할 수 없습니다. 5초 후에 재시도합니다...")
                    time.sleep(5)

            client.request_RT_data()
            client.show()
            sys.exit(app.exec_())
        else:
            print("indi 초기화 실패로 프로그램을 종료합니다.")
    except KeyboardInterrupt:
        print("\n프로그램을 종료합니다...")
    except Exception as e:
        print(f"프로그램 중 오류 발생: {e}")
    finally:
        client.socket.close()   # 소켓 연결 종료
        sys.exit(1)