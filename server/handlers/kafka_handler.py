from confluent_kafka import Producer
from config import Config
import json
import time
from confluent_kafka.admin import AdminClient, NewTopic

class KafkaHandler:
    def __init__(self):
        self.producer = self._create_producer()
        self.temp_storage = []  # 임시 저장소
        self._create_topics()  # 토픽 생성 확인
        
    def _create_producer(self):
        """Kafka Producer 생성"""
        conf = {
            'bootstrap.servers': Config.KAFKA_BOOTSTRAP_SERVERS,
            'queue.buffering.max.messages': 1000000,  # 충분한 버퍼 크기 유지
            'queue.buffering.max.kbytes': 1048576,  # 대역폭 제한 고려
            'batch.num.messages': 1000,  # 배치 크기 유지
            'linger.ms': 1,  # 낮은 지연 시간 유지
            'compression.type': 'lz4',  # 효율적 압축 유지
            'acks': 'all',  # 추가: 메시지 안정성 보장
            'retries': 5,  # 추가: 메시지 재전송 횟수 제한
            'retry.backoff.ms': 100,  # 추가: 재시도 대기 시간
            'delivery.timeout.ms': 120000,  # 추가: 메시지 전송 타임아웃
        }
        return Producer(conf)
        
    def send_data(self, data, topic):
        """데이터를 Kafka로 전송"""
        try:
            # JSON 직렬화
            message = json.dumps(data)
            
            # Kafka로 전송
            self.producer.produce(
                topic,
                value=message.encode('utf-8'),
                callback=self._delivery_report
            )
            # 콜백이 실행될 수 있도록 충분한 시간 제공
            self.producer.poll(1.0)  # 1초 대기
            
        except BufferError:
            # 버퍼가 가득 찼을 때 flush 수행
            self.producer.flush()
            # 재시도
            self.producer.produce(
                topic,
                value=message.encode('utf-8'),
                callback=self._delivery_report
            )
        except Exception as e:
            print(f"Kafka 전송 실패: {e}")
            self.temp_storage.append(data)
            
    def _delivery_report(self, err, msg):
        """메시지 전송 결과 콜백"""
        if err is not None:
            print(f'메시지 전송 실패: {err}')
        # else:
        #     print(f'메시지 전송 성공: {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')
        
    def _create_topics(self):
        """필요한 Kafka 토픽 생성"""
        try:
            admin_client = AdminClient({'bootstrap.servers': Config.KAFKA_BOOTSTRAP_SERVERS})
            topic_list = [NewTopic(
                topic=Config.KAFKA_TOPICS['RAW_MARKET_DATA'],
                num_partitions=6,
                replication_factor=1
            ), NewTopic(
                topic=Config.KAFKA_TOPICS['PROCESSED_DATA'],
                num_partitions=6,
                replication_factor=1
            )]
            
            fs = admin_client.create_topics(topic_list)
            for topic, f in fs.items():
                try:
                    f.result()  # 토픽 생성 완료 대기
                    print(f"토픽 생성 완료: {topic}")
                except Exception as e:
                    if "already exists" not in str(e):
                        raise e
                
        except Exception as e:
            print(f"토픽 생성 실패: {e}")