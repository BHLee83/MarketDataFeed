import time
import socket
import threading
import os
import sys
import psutil
import signal
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Config
from database.db_manager import DatabaseManager
from server.handlers.socket_handler import SocketHandler
from server.market_data_processor import MarketDataProcessor
from server.handlers.kafka_handler import KafkaHandler
from confluent_kafka import Producer
from server.monitoring.system_monitor import SystemMonitor
from server.processing_server import ProcessingServer
from utils.logger import server_logger


# app = Flask(__name__)
# class DataServer(Flask):
class DataServer():
    def __init__(self, host=Config.HOST, port=Config.PORT, monitor=None):
        # super().__init__(import_name=__name__, static_url_path='')  # static_url_path를 빈 문자열로 설정
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.settimeout(1.0)  # accept()에 대한 타임아웃만 설정
        self.host = host
        self.port = port
        self.clients = []
        self.client_data = {}  # 각 클라이언트별 수신된 데이터 저장
        self.dataset = []      # 통합 데이터셋
        self.active_clients = {}  # 활성 클라이언트 관리
        self.client_locks = {}    # 클라이언트별 락
        self.connection_status = {}  # 연결 상태 모니터링
        self.max_clients = Config.MAX_CLIENTS  # 최대 클라이언트 수 제한
        self.memory_limit = Config.MEMORY_LIMIT  # 메모리 사용량 제한
        self.last_db_write = time.time()  # 마지막 DB 기록 시점
        self.db_manager = DatabaseManager()
        self.socket_handler = SocketHandler()
        self.kafka_handler = KafkaHandler()
        self.marketdata_processor = MarketDataProcessor(
            self.client_data, 
            self.dataset,
            self.kafka_handler
        )
        self.running = True
        self.monitor = monitor
        # self.run(host=self.host, port=self.port, debug=True)
        
    def start(self):
        try:
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen()
            
            # 주기적인 DB 업데이트를 위한 스레드 시작
            self.idle_thread = threading.Thread(target=self.check_idle_time, daemon=True)
            self.idle_thread.start()
            
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

    def send_data_to_db(self, current_time):
        """데이터를 DB에 저장하는 메서드"""
        new_data = [data for data in self.dataset if data['received_at'] > self.last_db_write]
        if new_data:
            prepared_data = self.marketdata_processor.prepare_db_data(new_data)
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
                prepared_data = self.marketdata_processor.prepare_db_data(data)
                self.db_manager.insert_to_marketdata_price_rt(prepared_data)
            print(f"데이터셋이 DB에 저장되었습니다. 저장된 데이터 수: {len(data_to_store)}")

            # 데이터셋 비우고 남길 데이터만 유지
            self.dataset = data_to_keep
            self.last_db_write = current_time

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
                        print(f"[{datetime.now()}] {interval}초 기간 실시간 데이터 보관: {len(self.dataset)} 완료")
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

                # 데이터 처리
                processed_data = self.marketdata_processor.process_data(data, client_id)
                if processed_data:
                    self.kafka_handler.send_data(processed_data, topic=Config.KAFKA_TOPICS['RAW_MARKET_DATA']) # 실시간으로 Kafka에 전송
                    self.dataset.append(processed_data)    # 데이터셋에 추가

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
        
    # def monitor_memory_usage(self):
    #     """메모리 사용량 모니터링"""
    #     process = psutil.Process()
    #     memory_usage = process.memory_info().rss / 1024 / 1024  # MB
        
    #     if memory_usage > self.memory_limit:
    #         # 오래된 데이터 정리
    #         self.cleanup_old_data()

    # def cleanup_old_data(self):
    #     """오래된 데이터 정리"""
    #     # 오래된 데이터 기준 시간 설정 (예: 1시간 전)
    #     cutoff_time = time.time() - 3600  # 1시간 전

    #     # 오래된 데이터 제거
    #     self.dataset = [data for data in self.dataset if data['received_at'] > cutoff_time]
    #     print(f"오래된 데이터가 정리되었습니다. 남은 데이터 수: {len(self.dataset)}")

    def stop(self):
        """서버 종료 메서드"""
        self.running = False
        self.server_socket.close()
        
        # 실행 중인 모든 스레드 정리
        if hasattr(self, 'idle_thread') and self.idle_thread.is_alive():
            self.idle_thread.join(timeout=5)
        
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
            return False
        
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