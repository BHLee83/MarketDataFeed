import time
import socket
import threading
import os
import sys
import signal

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Config
from database.db_manager import DatabaseManager
from server.processing_server import ProcessingServer
from server.handlers.socket_handler import SocketHandler
from server.handlers.kafka_handler import KafkaHandler
from server.monitoring.system_monitor import SystemMonitor
from server.data_processor import DataProcessor
from utils.logger import server_logger


class DataServer():
    def __init__(self, host=Config.HOST, port=Config.PORT, monitor=None):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.settimeout(1.0)  # accept()에 대한 타임아웃만 설정
        self.host = host
        self.port = port
        self.clients = []
        self.client_data = {}  # 각 클라이언트별 수신된 데이터 저장
        self.active_clients = {}  # 활성 클라이언트 관리
        self.client_locks = {}    # 클라이언트별 락
        self.connection_status = {}  # 연결 상태 모니터링
        self.max_clients = Config.MAX_CLIENTS  # 최대 클라이언트 수 제한
        self.memory_limit = Config.MEMORY_LIMIT  # 메모리 사용량 제한
        self.db_manager = DatabaseManager()
        self.socket_handler = SocketHandler()
        self.kafka_handler = KafkaHandler()
        self.running = True
        self.monitor = monitor
        self.data_processor = DataProcessor(self.db_manager, self.socket_handler)
        
        # 데이터 처리 스레드 시작
        self.processor_thread = threading.Thread(target=self.data_processor.check_idle_time, daemon=True)
        self.processor_thread.start()
        
    def start(self):
        try:
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen()
            server_logger.info("서버가 시작되었습니다.")
            
            # 메인 서버 루프 실행
            self._run_server()
            
        except Exception as e:
            server_logger.error("서버 시작 오류: %s", e, exc_info=True)
            self.server_socket.close()
            sys.exit(1)
    
    def _run_server(self):
        """메인 서버 루프"""
        while self.running:
            try:
                # 새로운 클라이언트 연결 수락
                client_socket, addr = self.server_socket.accept()
                client_id = f"{addr[0]}:{addr[1]}"
                
                # 클라이언트 설정
                self.socket_handler.setup_client(client_socket, client_id)
                
                # 클라이언트 처리 스레드 시작
                client_thread = threading.Thread(
                    target=self.handle_client,
                    args=(client_socket, client_id),
                    daemon=True
                )
                client_thread.start()
                
                server_logger.info(f"새로운 클라이언트 연결: {client_id}")
                
            except socket.timeout:
                continue
            except Exception as e:
                server_logger.error(f"서버 루프 오류: {e}", exc_info=True)
                break

    def handle_client(self, client_socket, client_id):
        """클라이언트 연결 처리"""
        self.socket_handler.setup_client(client_socket, client_id)
        
        while self.running:
            try:
                data = self.socket_handler.receive_data(client_socket)
                if not data:
                    # 연결 실패 처리
                    if self.socket_handler.handle_connection_failure(client_id):
                        time.sleep(self.socket_handler.reconnect_delay)
                        continue
                    else:
                        break

                # 데이터 수신 성공 시 상태 업데이트
                self.socket_handler.update_client_status(client_id, 'connected')
                self.socket_handler.reconnect_attempts[client_id] = 0

                # 데이터 처리
                processed_data = self.data_processor.process_data(data, client_id)
                if processed_data:
                    self.data_processor.dataset.append(processed_data)  # 데이터셋에 추가
                    self.kafka_handler.send_data(topic=Config.KAFKA_TOPICS['RAW_MARKET_DATA'], data=processed_data) # 실시간으로 Kafka에 전송

            except Exception as e:
                server_logger.error(f"Client {client_id} error: {e}")
                if not self.socket_handler.handle_connection_failure(client_id):
                    break
                time.sleep(self.socket_handler.reconnect_delay)
                
        self.cleanup_client(client_id)

    def cleanup_client(self, client_id: str):
        """클라이언트 정리"""
        if client_id in self.client_data:
            del self.client_data[client_id]
        if client_id in self.socket_handler.client_locks:
            del self.socket_handler.client_locks[client_id]
        if client_id in self.socket_handler.connection_status:
            del self.socket_handler.connection_status[client_id]
        
    def stop(self):
        """서버 종료 메서드"""
        self.running = False
        self.server_socket.close()
        
        # 실행 중인 모든 스레드 정리
        if hasattr(self, 'processor_thread') and self.processor_thread.is_alive():
            self.processor_thread.join(timeout=5)
        
        # Kafka 핸들러 종료
        if hasattr(self, 'kafka_handler'):
            self.kafka_handler.producer.close()
        
        server_logger.info("서버가 안전하게 종료되었습니다.")


def start_data_server(system_monitor):
    """데이터 서버 시작"""
    data_server = DataServer(monitor=system_monitor)
    data_server.start()

def start_processing_server():
    """처리 서버 시작"""
    processing_server = ProcessingServer()
    processing_server.start()

if __name__ == "__main__":
    """서버 시작"""
    data_server = None
    processing_server = None

    try:
        # 단일 모니터링 인스턴스 생성
        system_monitor = SystemMonitor()
        system_monitor.start_monitoring()
        
        # 시그널 핸들러 설정
        def signal_handler(signum, frame):
            server_logger.info("종료 신호를 받았습니다. 서버를 종료합니다...")
            system_monitor.stop_monitoring()
            if data_server:
                data_server.stop()
            if processing_server:
                processing_server.stop()
            sys.exit(1)
            
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # 서버를 스레드로 시작
        server_logger.info("데이터 서버와 처리 서버를 시작합니다...")
        data_server_thread = threading.Thread(target=start_data_server, args=(system_monitor,))
        processing_server_thread = threading.Thread(target=start_processing_server)

        data_server_thread.start()
        processing_server_thread.start()

        # 메인 스레드에서 스레드가 종료될 때까지 대기
        data_server_thread.join()
        processing_server_thread.join()
        
    except KeyboardInterrupt:
        signal_handler(signal.SIGINT, None)

    except Exception as e:
        server_logger.error(f"서버 시작 중 오류 발생: {str(e)}", exc_info=True)