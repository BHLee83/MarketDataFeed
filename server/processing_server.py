import threading
from confluent_kafka import Consumer, KafkaError
from config import Config
from database.db_manager import DatabaseManager
from datetime import datetime
import time
import json
import os
from utils.logger import server_logger
from server.data_processor import DataProcessor
from server.handlers.kafka_handler import KafkaHandler

class ProcessingServer:
    def __init__(self):
        conf = {
            'bootstrap.servers': Config.KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'market_data_processor',
            'auto.offset.reset': 'earliest',  # 데이터 손실 방지를 위해 earliest 유지
            'max.partition.fetch.bytes': 10485760,  # 파티션별 최대 가져오기 크기 유지
            'socket.receive.buffer.bytes': 67108864,  # 소켓 버퍼 크기 유지
            'enable.auto.commit': True,  # 자동 커밋 유지
            'auto.commit.interval.ms': 1000,  # 커밋 간격을 유지, 더 낮출 수도 있음
            'session.timeout.ms': 30000,  # 연결 안정성을 위해 기본값 유지
            'heartbeat.interval.ms': 10000,  # 세션 타임아웃의 1/3로 유지
            'max.poll.interval.ms': 300000,  # 긴 처리 작업 허용
            'fetch.min.bytes': 1,  # 즉시 가져오기를 유지
            'fetch.wait.max.ms': 500,  # 낮은 대기 시간으로 빠른 가져오기
        }
        self.consumer = Consumer(conf)
        self.consumer.subscribe([Config.KAFKA_TOPICS['RAW_MARKET_DATA']])
        self.db_manager = DatabaseManager()
        self.kafka_handler = KafkaHandler()
        self.data_processor = DataProcessor(self.db_manager, self.kafka_handler, self.consumer)
        self.running = True

    def start(self):
        """데이터 처리 시작"""
        print("데이터 처리 서버가 시작되었습니다.")

        # self.data_processor.load_initial_data() # 기초 데이터 로드
        # self.dataset = self.data_processor.dataset   # DataProcessor에서 데이터셋 설정
        # while self.running:
        #     messages = self.consumer.consume(num_messages=100, timeout=1.0)
        #     for msg in messages:
        #         if not msg.error():
        #             market_data = json.loads(msg.value().decode('utf-8'))
        #             self.data_processor.process_kafka_data(market_data)
        
        self.data_processor.load_initial_data()  # 초기 데이터 로드
        while self.running:
            self.data_processor.manage_data_flow()

    def stop(self):
        """서버 종료"""
        self.running = False
        self.consumer.close()

if __name__ == "__main__":
    server = ProcessingServer()
    try:
        server.start()
    except KeyboardInterrupt:
        print("\n서버를 종료합니다...")
        server.stop()
