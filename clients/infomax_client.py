import win32com.client
import pythoncom
import time
import socket
import threading
from datetime import datetime, timedelta

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Config

from schemas.market_data import MarketData


class InfomaxClient:
    # 전역 변수로 Excel 관련 객체 선언

    def __init__(self, file_path, host=Config.SERVER_HOST, port=Config.SERVER_PORT):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.host = host
        self.port = port
        self.file_path = file_path
        self.excel_app = None
        self.workbook = None
        self.sheet = None
        self.previous_data = {}
        self.today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)

        # Start a thread to update the date
        self.date_update_thread = threading.Thread(target=self.update_today_date, daemon=True)
        self.date_update_thread.start()
    
    def update_today_date(self):
        while True:
            now = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
            if now != self.today:
                self.today = now
            time.sleep(3600)  # 1시간 마다 체크

    def initialize_excel(self):
        """Excel 초기화 및 설정"""
        try:
            print("Excel 초기화 시작...")
            # COM 초기화
            pythoncom.CoInitialize()

            # Excel 인스턴스 생성
            self.excel_app = win32com.client.DispatchEx("Excel.Application")
            self.excel_app.Visible = True
            
            # Infomax 추가기능 로드
            addin_path = r"C:\Users\infomax\AppData\Local\Infomax\bin\excel\infomaxexcel.xlam"
            addin_name = "infomaxexcel.xlam"
            
            # 추가기능 로드 시도
            for addon in self.excel_app.AddIns:
                if addon.Name.lower() == addin_name.lower():
                    addon.Installed = True
                    break
                    
            self.excel_app.Workbooks.Open(addin_path)
            time.sleep(1)  # 로드 대기
            
            # 워크북 열기
            if not os.path.exists(self.file_path):
                raise FileNotFoundError(f"File not found: {self.file_path}")
                
            self.workbook = self.excel_app.Workbooks.Open(self.file_path)
            time.sleep(5)  # 워크북 열기 대기
            
            self.sheet = self.workbook.Sheets(1)
            print("Excel 초기화 완료!")
            return True
        
        except Exception as e:
            print(f"Excel 초기화 중 오류 발생: {str(e)}")
            self.cleanup_excel()
            return False
    
    def connect_to_server(self):
        """서버에 연결 시도. 연결될 때까지 재시도"""
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
            
    def read_excel_data(self):
        """Excel 데이터 읽기"""
        try:
            last_row = self.sheet.Cells(self.sheet.Rows.Count, 1).End(-4162).Row
            current_data = self.sheet.Range(f"A4:C{last_row}").Value

            if not current_data:  # 데이터가 없는 경우
                return []

            # 변경된 데이터 추출
            updated_data = []
            for idx, row in enumerate(current_data):
                item_code, trd_time, current_price = row

                # current_price가 0인 경우 제외
                if current_price == 0:
                    continue

                row_data = (item_code, trd_time, current_price)

                # 이전 데이터와 비교하여 변경 사항이 있는 경우에만 저장
                if self.previous_data.get(idx) != row_data:
                    updated_data.append({
                        "row": idx,
                        "item_code": item_code,
                        "trd_time": (self.today + timedelta(days=trd_time)).isoformat(),
                        "current_price": current_price
                    })
                    self.previous_data[idx] = row_data  # 이전 데이터 업데이트

            # 업데이트된 데이터가 있는 경우에만 전송
            if updated_data:
                return updated_data
            return []
        
        except Exception as e:
            print(f"데이터 읽기 중 오류 발생: {str(e)}")
            return []
    
    # 데이터 구분자 추가
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
            'source': 'INFOMAX',
            'timestamp': datetime.now().isoformat(),
            'data_type': self._determine_data_type(raw_data),
            'content': raw_data
        }
        return processed_data
        
    def send_to_server(self, data):
        try:
            # Cap'n Proto 메시지 생성
            message = MarketData.MarketData.new_message()
            message.source = data['source']
            message.timestamp = data['timestamp']
            message.dataType = data['data_type']
            
            # content 리스트 설정
            content = message.init('content', len(data['content']))
            for i, item in enumerate(data['content']):
                content[i].itemCode = str(item['item_code'])
                content[i].trdTime = str(item['trd_time'])
                content[i].currentPrice = float(item['current_price'])
            
            # 직렬화
            serialized_data = message.to_bytes()
            
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

    def cleanup_excel(self):
        """Excel 리소스 정리"""
        try:
            if self.workbook:
                self.workbook.Save()
                self.workbook.Close(SaveChanges=True)
            if self.excel_app:
                self.excel_app.Quit()
        except Exception as e:
            print(f"cleanup 중 오류 발생: {str(e)}")
        finally:
            self.excel_app = None
            self.workbook = None
            self.sheet = None
            pythoncom.CoUninitialize()


if __name__ == "__main__":
    file_path = r"D:\Public\Infomax_API\AllList_RT.xlsx"
    client = InfomaxClient(file_path=file_path)
    try:
        # Excel 초기화 먼저 수행
        if client.initialize_excel():
            client.connect_to_server()
            # 데이터 전송 루프
            while True:
                raw_data = client.read_excel_data()
                if raw_data:
                    data = client.process_data(raw_data)
                    if not client.send_to_server(data):  # 전송 실패 시
                        print("서버 연결이 끊어졌습니다. 재연결을 시도합니다.")
                        client.socket.close()  # 기존 소켓 닫기
                        client.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # 새 소켓 생성
                        break  # 내부 루프를 빠져나가 재연결 시도
                    else:
                        print("서버로 데이터 전송:", data)
        else:
            print("Excel 초기화 실패로 프로그램을 종료합니다.")
    except KeyboardInterrupt:
        print("\n프로그램을 종료합니다...")
    finally:
        client.cleanup_excel()
        client.socket.close()  # 소켓 연결 종료