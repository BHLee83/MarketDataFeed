import cx_Oracle
from datetime import datetime

from PyQt5.QtWidgets import QApplication, QMainWindow, QLineEdit, QPushButton, QMessageBox
from PyQt5.QAxContainer import QAxWidget
from PyQt5.QtCore import QEventLoop, QTimer, Qt
from PyQt5.QtGui import QPalette, QColor

import sys
# import os
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Config

class SHiIndiClient(QMainWindow):
    def __init__(self):
        super().__init__()
        self.event_loop = QEventLoop()
        self.IndiTR = QAxWidget("SHINHANINDI.shinhanINDICtrl.1")
        self.IndiTR.ReceiveData.connect(self.ReceiveData)
        self.IndiTR.ReceiveSysMsg.connect(self.ReceiveSysMsg)
        self.mst_ready = False
        self.mst_names = []
        self.cnt_mst_rcv = 0
        self.code_list = [] # 전체 종목 리스트
        self.codes = [] # self.code_list에서 '단축코드'만 추출
        self.rqidD = {}
        self.strToday = datetime.today().strftime('%Y%m%d')
        self.strDtTarget = self.strToday
        self.eq_fut_code = '101'

        self.config = {
            'user': Config.DB_USER,
            'password': Config.DB_PASSWORD,
            'dsn': Config.DB_HOST
        }
        self.conn = cx_Oracle.connect(**self.config)
        self.cursor = self.conn.cursor()

    def initialize_indi(self):
        self.setWindowTitle("Market Data client")
        self.mst_names = ['FRF_MST']  # 종목코드 조회

        try:
            print("indi 초기화 시작")
            # indi login
            login = self.IndiTR.StartIndi(Config.SHi_indi_ID, Config.SHi_indi_PW, Config.SHi_indi_Auth, Config.SHi_indiGX_Path)
            if login:
                self.timer = QTimer()
                self.timer.timeout.connect(self.handle_timeout)
                self.timer.start(5000)
                self.event_loop.exec_()
                print("indi 로그인 프로세스 종료")
            else:
                print("indi 실행 실패!")
                return False
            
            self.rqList()  # 종목코드 리스트 수신
            if self.mst_ready:
                print(f"종목코드 리스트 수신 성공")
            
                self.leDate = QLineEdit(self)   # Target date
                self.leDate.setGeometry(20, 20, 60, 20)
                self.leDate.setText(self.strDtTarget)
                self.leDate.editingFinished.connect(self.leDateEdited)
                self.leDate.returnPressed.connect(self.btn_Insert)

                self.leTargets = QLineEdit(self)    # Target symbols
                self.leTargets.setGeometry(20, 50, 160, 20)
                self.leTargets.setText(', '.join(self.codes))

                self.leTargets.setReadOnly(True)
                palette = QPalette()
                palette.setColor(QPalette.Base, QColor(Qt.GlobalColor.gray))
                self.leTargets.setPalette(palette)

                btnInsert = QPushButton("Insert", self)
                btnInsert.setGeometry(85, 20, 50, 20)
                btnInsert.clicked.connect(self.btn_Insert)
                
                print("indi 초기화 성공")
                return True
            else:
                print("종목코드 리스트 수신 실패!")
                return False

        except Exception as e:
            print(f"indi 초기화 중 오류 발생: {e}")
            return False
        
    def btn_Insert(self):
        '버튼 클릭시 데이터 수집/입력 실행'
        self.cnt_daily_rcv = 0
        self.cnt_min_rcv = {'1m': 0, '3m': 0, '5m': 0, '10m': 0, '15m': 0, '30m': 0}
        self.data_daily = []    # 일봉 데이터
        self.data_min = {'1m': [], '3m': [], '5m': [], '10m': [], '15m': [], '30m': []}   # 분봉 데이터
        
        self.strDtTarget = self.leDate.text()
        if self.strDtTarget == self.strToday:
            print("데이터 요청 날짜가 오늘입니다")
            return
        if not self.mst_ready:
            print("종목코드 리스트 수신 필요")
            return

        self.rqDaily()  # 일 데이터 요청
        self.rqMinute() # 분 데이터 요청

        mbDone = QMessageBox(self)
        mbDone.setText('Done!')
        mbDone.exec_()

    def handle_timeout(self):
        # 타이머가 초과한 경우 처리
        print("Indi 로그인 시도 타임아웃!")
        self.timer.stop()  # 타이머 정지
        self.event_loop.exit()  # 이벤트 루프 종료
    
    def leDateEdited(self):
        self.strDtTarget = self.leDate.text()   # It must be 'YYYYMMDD' format

    def set_tr_chart_id(self, tr):
        if tr == 'FRF_MST':
            tr_chart_id = 'XTR_FFCHART'
        
        return tr_chart_id

    def rqList(self):
        """종목코드 리스트 요청"""
        for name in self.mst_names:
            ret = self.IndiTR.dynamicCall("SetQueryName(QString)", name)
            if ret:
                rqid = self.IndiTR.dynamicCall("RequestData()")
                self.rqidD[rqid] = name
                self.event_loop.exec_()
                
    def rqDaily(self):
        '일봉 데이터 요청'
        dataType = 'D'
        timeIntvl = '1'
        for i in self.code_list:
            tr_chart_id = self.set_tr_chart_id(i['구분'])
            ret = self.IndiTR.dynamicCall("SetQueryName(QString)", tr_chart_id)
            ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 0, i['단축코드'])  # 단축코드
            ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 1, dataType)    # 1: 분데이터, D:일데이터
            ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 2, timeIntvl)    # 시간간격 (분데이터일 경우 1-5, 일데이터일 경우 1)
            ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 3, self.strDtTarget) # YYYYMMDD (분 데이터 요청시: "00000000")
            ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 4, self.strDtTarget) # YYYYMMDD (분 데이터 요청시: "99999999")
            ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 5, '1')  # 조회갯수 (1 - 9999)
            rqid = self.IndiTR.dynamicCall("RequestData()")
            self.rqidD[rqid] =  tr_chart_id + '+' + dataType + '+' + i['단축코드']
            self.event_loop.exec_()

    def rqMinute(self):
        '분봉 데이터 요청'
        dataType = '1'
        timeIntvls = ['1', '5', '15', '30']   # 1분, 3분, 5분, 10분, 15분, 30분
        for i in timeIntvls:
            for j in self.code_list:
                tr_chart_id = self.set_tr_chart_id(j['구분'])
                ret = self.IndiTR.dynamicCall("SetQueryName(QString)", tr_chart_id)
                ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 0, j['단축코드'])  # 단축코드
                ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 1, dataType)    # 1: 분데이터, D:일데이터
                ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 2, i)    # 시간간격 (분데이터일 경우 1-5, 일데이터일 경우 1)
                ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 3, self.strDtTarget) # YYYYMMDD (분 데이터 요청시: "00000000")
                ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 4, self.strDtTarget) # YYYYMMDD (분 데이터 요청시: "99999999")
                ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 5, '9999')  # 조회갯수 (1 - 9999)
                rqid = self.IndiTR.dynamicCall("RequestData()")
                self.rqidD[rqid] = tr_chart_id + '+' + i + '+' + j['단축코드']
                self.event_loop.exec_()

    def procList(self, name):
        self.cnt_mst_rcv += 1
        cnt = self.IndiTR.dynamicCall("GetMultiRowCount()")
        print(f"종목코드 리스트 수신 중: {name}, {cnt}")
        if cnt > 0:
            for i in range(cnt):
                dictMst = {}
                if name == 'FRF_MST':
                    dictMst['구분'] = name
                    dictMst['단축코드'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 0)   # 종목코드
                    dictMst['종목명'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 1)
                self.code_list.append(dictMst)

        if self.cnt_mst_rcv == len(self.mst_names):
            self.code_list = [item for item in self.code_list if item]  # self.code_list 에서 값이 비어있는 건 제거
            new_codes = set()
            new_records = [
                {'구분': item['구분'], '단축코드': item['단축코드'][:-3], '종목명': f"{item['단축코드'][:-3]} 연결선물"}
                for item in self.code_list if '-' not in item['단축코드'] and item['단축코드'][:-3] not in new_codes and not new_codes.add(item['단축코드'][:-3])
            ]
            
            # 기존 데이터에 추가
            # self.code_list.extend(new_records)    # 애초에 연결선물만 요청하도록 변경
            self.code_list = new_records
            self.codes = [code['단축코드'] for code in self.code_list]
            self.mst_ready = True

    def procDaily(self, strTRChart, dataType, code):
        self.cnt_daily_rcv += 1
        cnt = self.IndiTR.dynamicCall("GetMultiRowCount()")
        print(f"일 데이터 수신 중: {code}, {cnt}")
        if cnt > 0:
            if strTRChart == 'XTR_FFCHART':
                for i in range(cnt):
                    data = {}
                    data['symbol'] = code
                    data['trd_date'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 0)    # 일자
                    data['trd_time'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 1)    # 체결시간
                    data['open'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 3)    # 시가
                    data['high'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 4)    # 고가
                    data['low'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 5)     # 저가
                    data['close'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 6)   # 현재가
                    data['open_int'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 8)   # 미결제약정수량
                    data['theo_prc'] = None
                    data['under_lvl'] = None
                    data['trd_volume'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 7)     # 단위거래량
                    data['trd_value'] = None
                    if float(data['close']) != 0:
                        self.data_daily.append(data)
        if self.cnt_daily_rcv == len(self.code_list):
            print("일 데이터 수신 완료")
            self.insert_to_db(f'1{dataType}', self.data_daily)

    def procMin(self, strTRChart, dataType, code):
        self.cnt_min_rcv[f'{dataType}m'] += 1
        cnt = self.IndiTR.dynamicCall("GetMultiRowCount()")
        if cnt > 0:
            print(f"{dataType}분 데이터 수신 중: {code}, {cnt}")
            if strTRChart == 'XTR_FFCHART':
                for i in range(cnt):
                    data = {}
                    data['trd_date'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 0)    # 일자
                    data['close'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 6)   # 현재가
                    if data['trd_date'] == self.strDtTarget and float(data['close']) != 0:
                        data['symbol'] = code
                        data['trd_time'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 1)    # 체결시간
                        data['open'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 3)    # 시가
                        data['high'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 4)    # 고가
                        data['low'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 5)     # 저가
                        data['open_int'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 8)   # 미결제약정수량
                        data['theo_prc'] = None
                        data['under_lvl'] = None
                        data['trd_volume'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 7)     # 단위거래량
                        data['trd_value'] = None
                        self.data_min[f'{dataType}m'].append(data)
        if self.cnt_min_rcv[f'{dataType}m'] == len(self.code_list):
            print(f"{dataType}분 데이터 수신 완료")
            self.insert_to_db(f'{dataType}m', self.data_min[f'{dataType}m'])

    def insert_to_db(self, dataType, data):
        if len(data) == 0:
            return
        
        # 쿼리 생성
        columns = data[0].keys()
        placeholders = ', '.join([f':{col}' for col in columns])
        query = f"""
        MERGE INTO marketdata_price_{dataType} target
        USING (SELECT {', '.join([f':{col} AS {col}' for col in columns])} FROM dual) source
        ON (target.symbol = source.symbol AND
            target.trd_date = source.trd_date AND
            target.trd_time = source.trd_time)
        WHEN MATCHED THEN
            UPDATE SET
                target.open = source.open,
                target.high = source.high,
                target.low = source.low,
                target.close = source.close,
                target.open_int = source.open_int,
                target.theo_prc = source.theo_prc,
                target.under_lvl = source.under_lvl,
                target.trd_volume = source.trd_volume,
                target.trd_value = source.trd_value
        WHEN NOT MATCHED THEN
            INSERT ({', '.join(columns)})
            VALUES ({placeholders})
        """
        
        try:
            data_tuples = [tuple(item.values()) for item in data]
            self.cursor.executemany(query, data_tuples)
            self.conn.commit()
            print(f"데이터 DB 저장 완료: {len(data_tuples)}건")
        except Exception as e:
            self.conn.rollback()
            print(f"DB 저장 중 오류 발생: {e}")
            print(f"쿼리: {query}")
            print(f"첫 번째 데이터 샘플: {data_tuples[0] if data_tuples else '없음'}")
    
    def ReceiveData(self, rqid):
        """데이터 수신시"""
        name = self.rqidD[rqid]

        if name in self.mst_names:  # 종목 정보
            self.procList(name)

        else:
            split_name = name.split('+')
            strTRChart = split_name[0]
            dataType = split_name[1]
            code = split_name[2]
            if dataType == 'D':   # 일봉
                self.procDaily(strTRChart, dataType, code)
            else:   # 분봉
                self.procMin(strTRChart, dataType, code)
        
        self.rqidD.__delitem__(rqid)
        self.event_loop.exit()

    def ReceiveSysMsg(self, MsgCode):
        if MsgCode == '11':
            print("시스템이 시작됨")
            # 메시지 수신 시 타이머 정지 및 이벤트 루프 종료
            self.timer.stop()
            self.event_loop.exit()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    client = SHiIndiClient()
    try:
        # indi 초기화 먼저 수행
        if client.initialize_indi():
            client.show()
            app.exec_()
        else:
            print("indi 초기화 실패로 프로그램을 종료합니다.")
    except KeyboardInterrupt:
        print("\n프로그램을 종료합니다...")
    except Exception as e:
        print(f"프로그램 중 오류 발생: {e}")
    finally:
        sys.exit(0)