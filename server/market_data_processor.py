from collections import defaultdict
from datetime import datetime
import time
from concurrent.futures import ThreadPoolExecutor
from utils.logger import server_logger
from config import Config

from schemas.market_data import MarketData
import json

class MarketDataProcessor:
    def __init__(self, client_data, dataset, kafka_handler):
        self.client_data = client_data
        self.dataset = dataset
        self.index = defaultdict(list)  # defaultdict로 변경
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.batch_size = 1000
        self.pending_data = []
        self.kafka_handler = kafka_handler  # 주입받은 handler 사용
        
    def is_capnp_data(self, data):
        """데이터가 Cap'n Proto 형식인지 확인하는 메서드"""
        # Cap'n Proto 데이터는 일반적으로 바이너리 형식이므로, 특정 바이트 패턴을 확인할 수 있습니다.
        # 예를 들어, 첫 번째 바이트가 특정 값인지 확인하는 방법
        return isinstance(data, bytes) and data.startswith(b'\x0a')  # 예시로 첫 바이트가 0x0a인지 확인

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

            # # 실시간으로 Kafka에 전송
            # self.kafka_handler.send_data(processed_data, topic=Config.KAFKA_TOPICS['RAW_MARKET_DATA'])

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
            
            # # 실시간으로 Kafka에 전송
            # self.kafka_handler.send_data(processed_data, topic=Config.KAFKA_TOPICS['RAW_MARKET_DATA'])
            
            # # 배치 처리를 위해 데이터 추가 (DB 저장용)
            # self.pending_data.append(processed_data)
            
            # # 배치 크기에 도달하면 비동기 처리
            # if len(self.pending_data) >= self.batch_size:
            #     self.executor.submit(self._process_batch)
                
            return processed_data
            
        except Exception as e:
            server_logger.error("데이터 처리 오류: %s", e, exc_info=True)
            return None
            
    def _process_batch(self):
        """배치 데이터 처리"""
        try:
            batch = self.pending_data
            self.pending_data = []
            
            # 소스별 데이터 그룹화
            source_groups = defaultdict(list)
            for data in batch:
                source_groups[data['source']].append(data)
                
            # 소스별로 한 번에 처리
            for source, data_list in source_groups.items():
                if source not in self.client_data:
                    self.client_data[source] = []
                self.client_data[source].extend(data_list)
                
            # 통합 데이터셋에 추가
            self.dataset.extend(batch)
            
            # 인덱스 일괄 업데이트
            for data in batch:
                for item in data['content']:
                    self.index[item['item_code']].append(data)
                    
        except Exception as e:
            server_logger.error("배치 처리 오류: %s", e, exc_info=True)

    def search_by_item_code(self, item_code):
        """인덱스를 사용하여 특정 item_code로 데이터를 검색"""
        return self.index.get(item_code, [])

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
                    'volume': 0
                })
        return db_records