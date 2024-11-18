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
            'queue.buffering.max.messages': 1000000,
            'queue.buffering.max.kbytes': 1048576,
            'batch.num.messages': 1000,
            'linger.ms': 5,
            'compression.type': 'lz4'
        }
        return Producer(conf)
        
    def send_market_data(self, data):
        """시장 데이터를 Kafka로 전송"""
        try:
            # JSON 직렬화
            message = json.dumps(data)
            
            # Kafka로 전송
            self.producer.produce(
                Config.KAFKA_TOPICS['RAW_MARKET_DATA'],
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
                Config.KAFKA_TOPICS['RAW_MARKET_DATA'],
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
                num_partitions=3,
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