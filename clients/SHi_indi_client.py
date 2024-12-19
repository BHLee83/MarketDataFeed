import time
import socket
from datetime import datetime

from PyQt5.QtWidgets import QApplication, QMainWindow
from PyQt5.QAxContainer import QAxWidget
from PyQt5.QtCore import QEventLoop, QTimer

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
        self.IndiTR = QAxWidget("SHINHANINDI.shinhanINDICtrl.1")
        self.IndiReal = QAxWidget("SHINHANINDI.shinhanINDICtrl.1")
        self.IndiTR.ReceiveData.connect(self.ReceiveData)
        self.IndiTR.ReceiveSysMsg.connect(self.ReceiveSysMsg)
        self.IndiReal.ReceiveRTData.connect(self.ReceiveRTData)
        self.mst_ready = False
        self.mst_names = []
        self.cnt_mst_rcv = 0
        self.code_list = []
        self.rqidD = {}
        self.today = datetime.today()

    def initialize_indi(self):
        self.setWindowTitle("Market Data client")
        self.mst_names = ['fut_mst', 'cfut_mst', 'fri_mst', 'gmf_mst']  # KOSPI 선물, 옵션, 상품선물, 유렉스, 해외지수, 야간달러선물 종목코드 조회

        try:
            print("indi 초기화 시작")
            # indi login
            login = self.IndiTR.StartIndi(Config.SHi_indi_ID, Config.SHi_indi_PW, Config.SHi_indi_Auth, Config.SHi_indi_Path)
            if login:
                self.timer = QTimer()
                self.timer.timeout.connect(self.handle_timeout)
                self.timer.start(10000)
                self.event_loop.exec_()
                print("indi 로그인 프로세스 종료")
            else:
                print("indi 실행 실패!")
                return False
            
            self.getCodeList()  # 종목코드 리스트 수신
            if self.mst_ready:
                print(f"종목코드 리스트 수신 성공!")
            else:
                print("종목코드 리스트 수신 실패!")
                return False
            
            print("indi 초기화 성공")
            return True

        except Exception as e:
            print(f"indi 초기화 중 오류 발생: {e}")
            return False
        
    def handle_timeout(self):
        # 타이머가 초과한 경우 처리
        print("Indi 로그인 시도 타임아웃!")
        self.timer.stop()  # 타이머 정지
        self.event_loop.exit()  # 이벤트 루프 종료

    def ReceiveSysMsg(self, MsgCode):
        if MsgCode == '11':
            print("시스템이 시작됨")
            # 메시지 수신 시 타이머 정지 및 이벤트 루프 종료
            self.timer.stop()
            self.event_loop.exit()

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
        if name in self.mst_names:
            self.cnt_mst_rcv += 1
            cnt = self.IndiTR.dynamicCall("GetMultiRowCount()")
            print(f"종목코드 리스트 수신: {name}, {cnt}")
            if cnt > 0:
                for i in range(cnt):
                    dictMst = {}
                    if name == 'fut_mst':
                        shortCode = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 1)
                        if shortCode.startswith('10'):  # 1: 선물 / 01: 코스피200, 04: 변동성, 05: 미니선물, 06: 코스닥, 07: 유로스톡스50, 08: KRX300
                            dictMst['구분']= name
                            dictMst['단축코드'] = shortCode
                            dictMst['종목명'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 2)
                            dictMst['기초자산ID'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 8)
                    elif name == 'cfut_mst':
                        dictMst['구분']= name
                        dictMst['단축코드'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 1)
                        dictMst['종목명'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 3)
                        dictMst['기초자산ID'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 4)
                    elif name == 'fri_mst':
                        dictMst['구분']= name
                        dictMst['단축코드'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 0)   # GIC
                        # dictMst['단축코드'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 2)   # 심벌
                        dictMst['종목명'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 4)
                    elif name == 'gmf_mst':
                        dictMst['구분']= name
                        dictMst['단축코드'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 1)
                        dictMst['종목명'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 2)
                        dictMst['기초자산ID'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 5)
                    self.code_list.append(dictMst)

            if self.cnt_mst_rcv == len(self.mst_names):
                self.code_list = [item for item in self.code_list if item]  # self.code_list 에서 값이 비어있는 건 제거
                # 수기 추가
                self.code_list.append({'구분': 'c_mst', '단축코드': 'USD00', '종목명': 'USDKRW'})   # USDKRW spot
                self.mst_ready = True

        self.event_loop.exit()

    def ReceiveRTData(self, RealType):
        if any([RealType == 'FC', RealType == 'MC']):
            item_code = self.IndiReal.dynamicCall("GetSingleData(int)", 1)    # 단축코드
            trade_time = self.IndiReal.dynamicCall("GetSingleData(int)", 2)    # 체결시간
            current_price = self.IndiReal.dynamicCall("GetSingleData(int)", 4)  # 현재가
            current_vol = self.IndiReal.dynamicCall("GetSingleData(int)", 10)  # 단위체결량
        elif any([RealType == 'IC', RealType == 'MX']):
            if RealType == 'IC':
                trade_time = self.IndiReal.dynamicCall("GetSingleData(int)", 1)    # 체결시간
            elif RealType == 'MX':
                trade_time = self.IndiReal.dynamicCall("GetSingleData(int)", 2)    # 체결시간
            item_code = self.IndiReal.dynamicCall("GetSingleData(int)", 0)    # 단축코드 or 업종코드
            current_price = self.IndiReal.dynamicCall("GetSingleData(int)", 3)  # 현재가
            current_vol = self.IndiReal.dynamicCall("GetSingleData(int)", 9)  # 단위체결량
        elif RealType == 'OC':
            print("야간 달러선물 데이터 수신됨. 작업 필요")

        data = [{'item_code': item_code, 'trade_time': trade_time, 'current_price': current_price, 'current_vol': current_vol}]
        print(f"수신데이터: {data}")
        processed_data = self.process_data(data)
        if not self.send_to_server(processed_data): # 전송 실패하면
            self.connect_to_server()    # 서버 연결 시도

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
    
    def process_data(self, data):
        # data = [
        #     item for item in raw_data
        #     if datetime.strptime(item['trade_time'], '%Y-%m-%d %H:%M:%S').date() == self.today
        # ]
        processed_data = {
            'source': 'SHINHANi',
            'timestamp': datetime.now().isoformat(),
            'data_type': self._determine_data_type(data),
            'content': data
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
            if client.connect_to_server():
                client.request_RT_data()
                client.show()
                app.exec_()
        else:
            print("indi 초기화 실패로 프로그램을 종료합니다.")
    except KeyboardInterrupt:
        print("\n프로그램을 종료합니다...")
    except Exception as e:
        print(f"프로그램 중 오류 발생: {e}")
    finally:
        client.socket.close()   # 소켓 연결 종료
        sys.exit(0)