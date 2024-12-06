import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Config
from confluent_kafka import Consumer, KafkaError
from database.db_manager import DatabaseManager
from utils.logger import server_logger
from server.handlers.kafka_handler import KafkaHandler
from server.market_data_processor import MarketDataProcessor

class ProcessingServer:
    def __init__(self):
        conf = {
            'bootstrap.servers': Config.KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'market_data_processor',
            'auto.offset.reset': 'earliest',  # 데이터 손실 방지를 위해 earliest
            'max.partition.fetch.bytes': 10485760,  # 파티션별 최대 가져오기 크기
            'socket.receive.buffer.bytes': 67108864,  # 소켓 버퍼 크기
            'enable.auto.commit': True,  # 자동 커밋
            'auto.commit.interval.ms': 1000,  # 커밋 간격, 더 낮출 수도 있음
            'session.timeout.ms': 30000,  # 연결 안정성을 위해
            'heartbeat.interval.ms': 10000,  # 세션 타임아웃의 1/3로
            'max.poll.interval.ms': 300000,  # 긴 처리 작업 허용
            'fetch.min.bytes': 1,  # 즉시 가져오기
            'fetch.wait.max.ms': 500,  # 낮은 대기 시간으로 빠른 가져오기
        }
        self.consumer = Consumer(conf)
        self.consumer.subscribe([Config.KAFKA_TOPICS['RAW_MARKET_DATA']])
        self.db_manager = DatabaseManager()
        self.kafka_handler = KafkaHandler()
        
        # MarketDataProcessor 초기화
        self.market_processor = MarketDataProcessor(
            self.db_manager, 
            self.kafka_handler, 
            self.consumer
        )
        self.running = True

    def start(self):
        """데이터 처리 시작"""
        server_logger.info("데이터 처리 서버가 시작되었습니다.")
        
        try:
            while self.running:
                # MarketDataProcessor에서 데이터 처리별 스레드 생성으로 여기서 따로 해줄건 없음
                pass

        except KeyboardInterrupt:
            server_logger.info("\n서버를 종료합니다...")
            self.stop()
            
        except Exception as e:
            server_logger.error(f"처리 서버 오류: {e}")
            self.stop()

    def stop(self):
        """서버 종료"""
        self.running = False
        self.market_processor.stop()  # MarketDataProcessor 종료
        self.consumer.close()

if __name__ == "__main__":
    server = ProcessingServer()
    try:
        server.start()
    except KeyboardInterrupt:
        server_logger.info("\n데이터 처리 서버를 종료합니다...")
        server.stop()
