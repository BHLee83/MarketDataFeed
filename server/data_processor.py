from datetime import datetime
import time
import psutil
import json

from config import Config
from utils.logger import server_logger
from schemas.market_data import MarketData

class DataProcessor:
    def __init__(self, db_manager, socket_handler):
        self.db_manager = db_manager
        self.socket_handler = socket_handler
        self.last_db_write = time.time()
        self.dataset = []
        self.running = True
        
    def check_idle_time(self):
        """
        CPU 사용률 30% 이하, 메모리 여유 30% 이상일 때
        프로그램 시작 시점부터 10분 단위로 메모리 데이터를 임시 테이블에 기록
        """
        
        # 프로그램 시작 시간 기록
        start_time = time.time()
        interval = Config.INTERVAL  # 10분 = 600초
        
        while self.running:  # running 플래그 확인
            try:
                # CPU, 메모리 사용률 체크
                cpu_usage = psutil.cpu_percent(interval=1)
                memory = psutil.virtual_memory()
                memory_available = memory.available * 100 / memory.total
                
                # idle 상태 체크
                if cpu_usage <= Config.CPU_USAGE and memory_available >= Config.MEMORY_ABAILABLE:
                    # 시작 시간부터 interval 단위 경과 체크
                    elapsed_time = time.time() - start_time
                    if elapsed_time >= interval:
                        # DB 저장 로직 호출
                        self.send_data_to_db(time.time())
                        print(f"[{datetime.now()}] {interval}초 기간 실시간 데이터 보관: 현재까지 총 {len(self.dataset)}개")
                        # 다음 interval 주기를 위해 시작 시간 갱신
                        start_time = time.time()
                
                # 데이터셋 만료 체크 및 저장
                self.check_and_store_data()

                # 1분 대기
                time.sleep(60)
                
            except Exception as e:
                print(f"데이터 이관 중 오류 발생: {e}")
                if not self.running:  # 종료 중이면 루프 탈출
                    break
                time.sleep(60)

    def send_data_to_db(self, current_time):
        """데이터를 DB에 저장하는 메서드"""
        new_data = [data for data in self.dataset if data['received_at'] > self.last_db_write]
        if new_data:
            prepared_data = self.prepare_db_data(new_data)
            self.db_manager.insert_to_marketdata_price_rt(prepared_data)
            self.last_db_write = current_time
            print(f"새로운 데이터 {len(new_data)}개가 DB에 저장되었습니다.")
    
    def check_and_store_data(self):
        """데이터셋을 DB로 전송하고 비우는 메서드"""
        if not self.dataset:
            return

        # 데이터셋의 첫 번째 데이터의 날짜를 기준으로 다음날인지 확인
        first_data_date = datetime.fromisoformat(self.dataset[0]['timestamp']).date()
        current_date = datetime.now().date()
        current_time = time.time()

        if current_date > first_data_date:
            # 전송할 데이터와 남길 데이터 분리
            data_to_store = [data for data in self.dataset if datetime.fromisoformat(data['timestamp']).date() < current_date]
            data_to_keep = [data for data in self.dataset if datetime.fromisoformat(data['timestamp']).date() == current_date]

            # DB에 저장
            for data in data_to_store:
                prepared_data = self.prepare_db_data(data)
                self.db_manager.insert_to_marketdata_price_rt(prepared_data)
            print(f"데이터셋이 DB에 저장되었습니다. 저장된 데이터 수: {len(data_to_store)}")

            # 데이터셋 비우고 남길 데이터만 유지
            self.dataset = data_to_keep
            self.last_db_write = current_time

    def is_capnp_data(self, data):
        """데이터가 Cap'n Proto 형식인지 확인하는 메서드"""
        # Cap'n Proto 데이터는 일반적으로 바이너리 형식이므로, 특정 바이트 패턴을 확인할 수 있습니다.
        # 예를 들어, 첫 번째 바이트가 특정 값인지 확인하는 방법
        return isinstance(data, bytes) and data.startswith(b'\x00')  # 예시로 첫 바이트가 0x00인지 확인

    def process_data(self, data, client_id):
        if self.is_capnp_data(data):
            processed_data = self.process_capnp_data(data, client_id)
        else:
            processed_data = self.process_json_data(data, client_id)

        return processed_data

    def process_capnp_data(self, data, client_id):
        """Cap'n Proto 데이터 처리"""
        with MarketData.MarketData.from_bytes(data) as market_data:
            if not self.socket_handler.validate_market_data(market_data):
                server_logger.warning(f"Invalid Cap'n Proto data from {client_id}")
                return

            processed_data = self._process_data(market_data)

            return processed_data

    def process_json_data(self, data, client_id):
        """JSON 데이터 처리"""
        try:
            current_time = time.time()
            if isinstance(data, bytes):
                data = data.decode('utf-8')
            json_data = json.loads(data)  # JSON 문자열을 파싱
            # JSON 데이터 처리 로직 추가
            processed_data = {
                'source': json_data['source'],
                'timestamp': json_data['timestamp'],
                'data_type': json_data['data_type'],
                'content': [{
                    'item_code': item['item_code'],
                    'current_price': float(item['current_price']),
                    'current_vol': float(item['current_vol'])
                } for item in json_data['content']],
                'received_at': current_time}

            return processed_data
        except json.JSONDecodeError as e:
            server_logger.warning(f"Invalid JSON data from {client_id}: {e}")

    def _process_data(self, market_data):
        try:
            current_time = time.time()
            processed_data = {
                'source': str(market_data.source),
                'timestamp': str(market_data.timestamp),
                'data_type': str(market_data.dataType),
                'content': [{
                    'item_code': str(item.itemCode),
                    'current_price': float(item.currentPrice)
                } for item in market_data.content],
                'received_at': current_time
            }

            return processed_data
            
        except Exception as e:
            server_logger.error("데이터 처리 오류: %s", e, exc_info=True)
            return None
    
    def prepare_db_data(self, new_data):
        """DB 저장을 위한 데이터 형식 변환"""
        db_records = []
        for data in new_data:
            # ISO 형식의 timestamp를 파싱 (예: 2024-11-13T17:13:34.622521)
            dt = datetime.fromisoformat(data['timestamp'])
            formatted_timestamp = dt.strftime('%Y-%m-%d %H:%M:%S.%f')
            
            for item in data['content']:
                db_records.append({
                    'symbol': item['item_code'],
                    'timestamp': formatted_timestamp,
                    'price': item['current_price'],
                    'volume': item['current_vol']
                })
        return db_records