from collections import defaultdict
from datetime import datetime
import time
from concurrent.futures import ThreadPoolExecutor
from server.utils.logger import server_logger

class MarketDataProcessor:
    def __init__(self, client_data, dataset, kafka_handler):
        self.client_data = client_data
        self.dataset = dataset
        self.index = defaultdict(list)  # defaultdict로 변경
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.batch_size = 1000
        self.pending_data = []
        self.kafka_handler = kafka_handler  # 주입받은 handler 사용
        
    def process_data(self, market_data):
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
            
            # 실시간으로 Kafka에 전송
            self.kafka_handler.send_market_data(processed_data)
            
            # 배치 처리를 위해 데이터 추가 (DB 저장용)
            self.pending_data.append(processed_data)
            
            # 배치 크기에 도달하면 비동기 처리
            if len(self.pending_data) >= self.batch_size:
                self.executor.submit(self._process_batch)
                
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