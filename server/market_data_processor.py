from datetime import datetime, timedelta
import time
import json
import threading
from config import Config
from utils.logger import server_logger

class MarketDataProcessor:
    def __init__(self, db_manager, kafka_handler, consumer):
        self.db_manager = db_manager
        self.kafka_handler = kafka_handler
        self.consumer = consumer
        self.running = True
        self.topic = Config.KAFKA_TOPICS['PROCESSED_DATA']
        
        # 캐시 데이터 크기 제한
        self.MAX_CHUNK_SIZE = 100  # 한 번에 보낼 수 있는 최대 데이터 수

        # 시계열 데이터 처리를 위한 캐시
        self.time_series_cache = {
            'tick': [],    # 체결 데이터
            'minute': {},  # 분봉 데이터
            'daily': {}    # 일봉 데이터
        }
        
        # 통계 데이터 캐시
        self.statistics_cache = {
            'volume_profile': {},  # 거래량 프로파일
            'price_levels': {},    # 가격대별 거래량
            'moving_averages': {}  # 이동평균
        }
        
        # 차트 데이터 캐시
        self.chart_cache = {}
        
        # 처리 스레드 시작
        self.start_processing_threads()
        
    def start_processing_threads(self):
        """데이터 처리 스레드 시작"""
        self.threads = {
            'time_series': threading.Thread(target=self.process_time_series, daemon=True),
            'statistics': threading.Thread(target=self.process_statistics, daemon=True),
            'chart_data': threading.Thread(target=self.process_chart_data, daemon=True),
            'publish_processed_data': threading.Thread(target=self.publish_processed_data, daemon=True)
        }
        
        for thread in self.threads.values():
            thread.start()

    def stop(self):
        """데이터 처리 스레드 종료"""
        server_logger.info("\n서버를 종료합니다...")
        
        self.running = False
        for thread in self.threads.values():
            thread.join()

    def process_time_series(self):
        """시계열 데이터 처리"""
        while self.running:
            try:
                messages = self.consumer.consume(num_messages=100, timeout=1.0)
                for msg in messages:
                    if msg.error():
                        continue
                        
                    market_data = json.loads(msg.value().decode('utf-8'))
                    self.update_time_series(market_data)
                    
                # 주기적으로 캐시 데이터를 DB에 저장
                self.store_time_series_data()
                    
            except Exception as e:
                print(f"시계열 데이터 처리 오류: {e}")
                time.sleep(1)
                
    def process_statistics(self):
        """통계 데이터 처리"""
        while self.running:
            try:
                self.calculate_volume_profile()
                self.calculate_price_levels()
                self.calculate_moving_averages()
                time.sleep(1)  # 1초마다 통계 업데이트
                
            except Exception as e:
                print(f"통계 데이터 처리 오류: {e}")
                time.sleep(1)
                
    def process_chart_data(self):
        """차트 데이터 생성"""
        while self.running:
            try:
                self.generate_chart_data()
                time.sleep(0.5)  # 0.5초마다 차트 데이터 업데이트
                
            except Exception as e:
                print(f"차트 데이터 처리 오류: {e}")
                time.sleep(1)
                
    def _chunk_data(self, data):
        """대용량 데이터를 작은 청크로 분할"""
        for i in range(0, len(data), self.MAX_CHUNK_SIZE):
            yield data[i:i + self.MAX_CHUNK_SIZE]

    def publish_processed_data(self):
        """처리된 데이터를 Kafka의 processed-data 토픽에 발행"""
        while self.running:
            try:
                # 시계열 데이터 발행 (청크 단위)
                if self.time_series_cache['tick']:
                    for tick_chunk in self._chunk_data(self.time_series_cache['tick']):
                        processed_tick_data = {
                            'type': 'tick_data',
                            'data': tick_chunk
                        }
                        self.kafka_handler.send_data(
                            topic=self.topic, 
                            data=processed_tick_data
                        )
                    # 발행 후 캐시 초기화
                    self.time_series_cache['tick'] = []

                # 분봉 데이터 발행 (청크 단위)
                for minute_key, minute_data in list(self.time_series_cache['minute'].items()):
                    for minute_chunk in self._chunk_data(minute_data):
                        processed_minute_data = {
                            'type': 'minute_data',
                            'key': minute_key,
                            'data': minute_chunk
                        }
                        self.kafka_handler.send_data(
                            topic=self.topic, 
                            data=processed_minute_data
                        )
                    # 발행 후 해당 분봉 데이터 삭제
                    del self.time_series_cache['minute'][minute_key]

                # 통계 데이터 발행 (청크 단위)
                if self.statistics_cache:
                    processed_stats_data = {
                        'type': 'statistics_data',
                        'data': self.statistics_cache
                    }
                    self.kafka_handler.send_data(
                        topic=self.topic, 
                        data=processed_stats_data
                    )

                # 차트 데이터 발행 (필요시 청크 처리)
                if self.chart_cache:
                    processed_chart_data = {
                        'type': 'chart_data',
                        'data': self.chart_cache
                    }
                    self.kafka_handler.send_data(
                        topic=self.topic, 
                        data=processed_chart_data
                    )

                # 일정 간격으로 발행
                time.sleep(5)  # 5초마다 처리된 데이터 발행

            except Exception as e:
                server_logger.error(f"처리된 데이터 발행 오류: {e}")
                time.sleep(1)

    def update_time_series(self, market_data):
        """시계열 데이터 업데이트"""
        timestamp = datetime.fromisoformat(market_data['timestamp'])
        
        # 체결 데이터 업데이트
        self.time_series_cache['tick'].append(market_data)
        
        # 분봉 데이터 업데이트
        minute_key = timestamp.strftime('%Y%m%d%H%M')
        if minute_key not in self.time_series_cache['minute']:
            self.time_series_cache['minute'][minute_key] = []
        self.time_series_cache['minute'][minute_key].append(market_data)
        
        # 일봉 데이터 업데이트
        daily_key = timestamp.strftime('%Y%m%d')
        if daily_key not in self.time_series_cache['daily']:
            self.time_series_cache['daily'][daily_key] = []
        self.time_series_cache['daily'][daily_key].append(market_data)
        
    def store_time_series_data(self):
        """시계열 데이터 DB 저장"""
        current_time = datetime.now()
        
        # 전일 데이터 저장 (새벽 5시)
        if current_time.hour == 5 and current_time.minute == 0:
            previous_day = (current_time - timedelta(days=1)).strftime('%Y%m%d')
            if previous_day in self.time_series_cache['daily']:
                self.db_manager.store_daily_data(
                    self.time_series_cache['daily'][previous_day]
                )
                del self.time_series_cache['daily'][previous_day]

    # 아직 구현되지 않은 메서드들 (플레이스홀더)
    def calculate_volume_profile(self):
        """거래량 프로파일 계산"""
        pass
    
    def calculate_price_levels(self):
        """가격대별 거래량 계산"""
        pass
    
    def calculate_moving_averages(self):
        """이동평균 계산"""
        pass
    
    def generate_chart_data(self):
        """차트 데이터 생성"""
        pass