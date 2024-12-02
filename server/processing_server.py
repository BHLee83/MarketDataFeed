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
            'auto.offset.reset': 'earliest',  # latest -> earliest로 변경
            'max.partition.fetch.bytes': 10485760,
            'fetch.message.max.bytes': 10485760,
            'socket.receive.buffer.bytes': 67108864,
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 1000,  # 5000 -> 1000으로 변경
            'session.timeout.ms': 30000,
            'heartbeat.interval.ms': 10000,
            'max.poll.interval.ms': 300000,  # 추가: 긴 처리 시간 허용
            'fetch.min.bytes': 1,  # 추가: 즉시 데이터 가져오기
            'fetch.wait.max.ms': 500  # 추가: 최대 대기 시간 제한
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
