from confluent_kafka import Producer, KafkaError
from config import Config
import json
import time
from utils.logger import kafka_logger
from confluent_kafka.admin import AdminClient, NewTopic
from typing import Dict, Any, List
import threading
import queue

class KafkaHandler:
    def __init__(self, max_retries=10, retry_backoff_base=0.5):
        # 메시지 재시도 관련 설정
        self.max_retries = max_retries
        self.retry_backoff_base = retry_backoff_base

        # 안전한 메시지 큐 생성
        self.message_queue = queue.Queue(maxsize=1000000)

        # Producer 생성 및 백그라운드 스레드 시작
        self.producer = self._create_producer()
        # self.temp_storage = []  # 임시 저장소
        self._start_message_sender()

        self._create_topics()  # 토픽 생성 확인
        
    def _create_producer(self) -> Producer:
        """Kafka Producer 생성"""
        conf = {
            'bootstrap.servers': Config.KAFKA_BOOTSTRAP_SERVERS,
            'queue.buffering.max.messages': 500000,  # 버퍼 크기 최적화
            'queue.buffering.max.kbytes': 102400,   # 메모리 관리
            'batch.num.messages': 10000,            # 대량 배치 처리
            'linger.ms': 50,                        # 짧은 대기 시간
            'compression.type': 'lz4',             # 효율적 압축
            'acks': 'all',                          # 메시지 안정성 보장
            'retries': 3,                           # 재시도 횟수 제한
            'retry.backoff.ms': 300,                # 재시도 간 대기 시간
            'delivery.timeout.ms': 30000,           # 전송 타임아웃
            'max.in.flight.requests.per.connection': 5  # 동시 요청 제한
        }
        return Producer(conf)
        
    def send_data(self, topic: str, data: Dict[str, Any]):
        """데이터를 Kafka로 전송"""
        try:
            # JSON 직렬화 및 크기 제한 확인
            try:
                message = json.dumps(data).encode('utf-8')
            except Exception as e:
                kafka_logger.error(f"JSON 직렬화 실패: {e}")
                return

            # 메시지 크기 로깅 (디버깅용)
            if len(message) > 10 * 1024 * 1024:  # 10MB 초과 시 경고
                kafka_logger.warning(f"대용량 메시지 감지: {len(message)} bytes")

            # 메시지 큐에 추가 (백그라운드 스레드에서 처리)
            self.message_queue.put((topic, message, 0))
            
        except Exception as e:
            kafka_logger.error(f"메시지 큐 추가 실패: {e}")
            
    def _message_sender_thread(self):
        """백그라운드 메시지 전송 스레드"""
        while True:
            try:
                topic, message, retry_count = self.message_queue.get()
                
                # 재시도 로직 포함
                try:
                    self.producer.produce(
                        topic, 
                        value=message, 
                        callback=self._delivery_report
                    )
                    self.producer.poll(0)  # 비차단 폴링
                    
                except BufferError:
                    # 버퍼 대기 후 재시도
                    time.sleep(0.1)
                    if retry_count < self.max_retries:
                        self.message_queue.put((topic, message, retry_count + 1))
                    else:
                        kafka_logger.error(f"최대 재시도 횟수 초과: {topic}")
                
            except Exception as e:
                kafka_logger.error(f"메시지 전송 스레드 오류: {e}")
                time.sleep(1)
            
    def _start_message_sender(self):
        """백그라운드 메시지 전송 스레드 시작"""
        sender_thread = threading.Thread(target=self._message_sender_thread, daemon=True)
        sender_thread.start()

    def _delivery_report(self, err, msg):
        """메시지 전송 결과 콜백"""
        if err is not None:
            kafka_logger.error(f'메시지 전송 실패: {err}')
        # else:
        #     print(f'메시지 전송 성공: {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')
        
    def _create_topics(self):
        """동적 토픽 생성 및 관리"""
        try:
            admin_client = AdminClient({'bootstrap.servers': Config.KAFKA_BOOTSTRAP_SERVERS})

            # 동적 파티션 수 계산 (시스템 리소스 고려)
            import multiprocessing
            cpu_count = multiprocessing.cpu_count()
            partition_count = max(cpu_count * 2, 3)  # 최소 3개, CPU 코어 수의 2배

            topic_list = [NewTopic(
                topic=Config.KAFKA_TOPICS['RAW_MARKET_DATA'],
                num_partitions=partition_count,
                replication_factor=1
            ), NewTopic(
                topic=Config.KAFKA_TOPICS['RAW_MARKET_DATA_MINUTE'],
                num_partitions=partition_count,
                replication_factor=1
            ), NewTopic(
                topic=Config.KAFKA_TOPICS['RAW_MARKET_DATA_DAY'],
                num_partitions=partition_count,
                replication_factor=1
            )]
            
            fs = admin_client.create_topics(topic_list)
            for topic, f in fs.items():
                try:
                    f.result()  # 토픽 생성 완료 대기
                    kafka_logger.info(f"토픽 생성 완료: {topic}")
                except Exception as e:
                    if "already exists" not in str(e):
                        raise e
                
        except Exception as e:
            kafka_logger.error(f"토픽 생성 실패: {e}")

    def close(self):
        """안전한 종료 메서드"""
        self.producer.flush()  # 남은 모든 메시지 전송
        self.producer.close()  # Producer 닫기