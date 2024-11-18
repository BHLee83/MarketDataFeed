import os
import sys

# 프로젝트 루트 디렉토리를 Python 경로에 추가
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from confluent_kafka.admin import AdminClient
from confluent_kafka import KafkaException
import socket
import time
import signal
import threading
from server.main_server import DataServer
from server.processing_server import ProcessingServer
from server.utils.logger import setup_logger
from server.monitoring.system_monitor import SystemMonitor

logger = setup_logger('servers')

def check_kafka_connection():
    """Kafka 연결 상태 확인"""
    try:
        conf = {'bootstrap.servers': 'localhost:9092'}
        admin_client = AdminClient(conf)
        metadata = admin_client.list_topics(timeout=5)
        logger.info(f"Kafka 연결 성공 - 사용 가능한 토픽: {metadata.topics}")
        return True
    except KafkaException as e:
        logger.error(f"Kafka 연결 실패: {str(e)}")
        return False

def start_servers():
    """서버 시작"""
    if not check_kafka_connection():
        logger.error("Kafka 연결 실패로 서버를 시작할 수 없습니다")
        return False

    try:
        # 단일 모니터링 인스턴스 생성
        system_monitor = SystemMonitor()
        system_monitor.start_monitoring()
        
        # 서버 인스턴스 생성 (모니터링 공유)
        data_server = DataServer(monitor=system_monitor)
        processing_server = ProcessingServer()
        
        # 시그널 핸들러 설정
        def signal_handler(signum, frame):
            logger.info("종료 신호를 받았습니다. 서버를 종료합니다...")
            system_monitor.stop_monitoring()
            data_server.stop()
            processing_server.stop()
            
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # 서버 시작
        logger.info("데이터 서버와 처리 서버를 시작합니다...")
        data_server.start()
        processing_server.start()
        
        return True
        
    except Exception as e:
        logger.error(f"서버 시작 중 오류 발생: {str(e)}", exc_info=True)
        return False

if __name__ == "__main__":
    if not start_servers():
        sys.exit(1) 