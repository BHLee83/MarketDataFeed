from config import Config
from confluent_kafka import Consumer, KafkaError, TopicPartition, KafkaException
from confluent_kafka.admin import AdminClient
import threading
import time
import psutil
import json
from datetime import datetime
from utils.logger import monitoring_logger
from server.monitoring.alert_manager import AlertManager
from collections import defaultdict
from server.monitoring.metrics_exporter import MetricsExporter

class SystemMonitor:
    def __init__(self):
        self.consumer_conf = {
            'bootstrap.servers': Config.KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'market_data_monitor',
            'auto.offset.reset': 'earliest',  # 데이터 손실 방지를 위해 earliest 유지
            'max.partition.fetch.bytes': 10485760,  # 파티션별 최대 가져오기 크기 유지
            'socket.receive.buffer.bytes': 67108864,  # 소켓 버퍼 크기 유지
            'enable.auto.commit': True,  # 자동 커밋 유지
            'auto.commit.interval.ms': 1000,  # 커밋 간격을 유지, 더 낮출 수도 있음
            'session.timeout.ms': 30000,  # 연결 안정성을 위해 기본값 ���지
            'heartbeat.interval.ms': 10000,  # 세션 타임아웃의 1/3로 유지
            'max.poll.interval.ms': 300000,  # 긴 처리 작업 허용
            'fetch.min.bytes': 1,  # 즉시 가져오기를 유지
            'fetch.wait.max.ms': 500,  # 낮은 대기 시간으로 빠른 가져오기
        }
        
        self.admin_client = AdminClient({'bootstrap.servers': Config.KAFKA_BOOTSTRAP_SERVERS})
        self.topics = [Config.KAFKA_TOPICS['RAW_MARKET_DATA']]
        self.consumer = Consumer(self.consumer_conf)
        self.consumer.subscribe([self.topics], on_assign=self._on_assign)
        
        self.running = True
        self.metrics = defaultdict(dict)
        self.monitor_thread = None
        self.alert_manager = AlertManager()
        self.metrics_exporter = MetricsExporter()
        self.last_consumed_messages = []
        
    def _on_assign(self, consumer, partitions):
        """파티션 할당 시 호출되는 콜백"""
        for partition in partitions:
            try:
                # 커밋된 오프셋 조회
                committed = consumer.committed([partition], timeout=5.0)
                if committed and committed[0] and committed[0].offset > 0:
                    partition.offset = committed[0].offset
                else:
                    # 커밋된 오프셋이 없는 경우 가장 최근 오프셋으로 설정
                    _, high = consumer.get_watermark_offsets(partition)
                    partition.offset = high
            except KafkaException:
                # 오류 발생 시 가장 최근 오프셋 사용
                _, high = consumer.get_watermark_offsets(partition)
                partition.offset = high
        
    def _consume_messages(self):
        """메시지 소비 공통 로직"""
        try:
            messages = self.consumer.consume(num_messages=100000, timeout=5.0)
            monitoring_logger.info(f"소비된 메시지 수: {len(messages)}")
            self.last_consumed_messages = messages  # 메시지 캐시 업데이트
            return messages
        except Exception as e:
            monitoring_logger.error(f"메시지 소비 오류: {e}")
            return []

    def monitor_kafka(self):
        """Kafka 큐 모니터링"""
        try:
            # 메시지 소비
            messages = self._consume_messages()
            if messages:
                self.last_consumed_messages = messages
            
            current_time = time.time()
            
            # 파티션 정보 업데이트 (10초마다)
            if not hasattr(self, '_last_partition_update') or \
               current_time - getattr(self, '_last_partition_update', 0) > 10:
                
                try:
                    # 실제 존재하는 파티션 정보만 가져오기
                    cluster_metadata = self.admin_client.list_topics(timeout=10)
                    self._partitions = {}

                    for topic in self.topics:
                        if topic not in cluster_metadata.topics:
                            monitoring_logger.warning(f"토픽을 찾을 수 없음: {topic}")
                            continue
                        topic_metadata = cluster_metadata.topics[topic]
                        self._partitions[topic] = {
                            p_id: p_meta for p_id, p_meta in topic_metadata.partitions.items()
                        }

                    if self._partitions:
                        self._last_partition_update = current_time
                except KafkaException as ke:
                    monitoring_logger.error(f"토픽 메타데이터 조회 실패: {ke}")
                    return

                # 각 토픽의 실제 파티션에 대한 TopicPartition 객체 생성
                tps = []
                for topic, partitions in self._partitions.items():
                    tps.extend([TopicPartition(topic, p) for p in partitions.keys()])

                # 커밋된 오프셋 조회 전에 consumer assign
                self.consumer.assign(tps)
                
                # 각 파티션의 시작 오프셋으로 위치 설정
                low_offsets = {}  # 시작 오프셋 저장
                for tp in tps:
                    try:
                        # 파티션의 시작 오프셋 조회
                        low_offset, _ = self.consumer.get_watermark_offsets(tp)
                        low_offsets[tp] = low_offset
                        # TopicPartition에 offset 설정 후 seek
                        tp.offset = low_offset
                        self.consumer.seek(tp)
                    except KafkaException as e:
                        monitoring_logger.error(f"파티션 {tp.topic}[{tp.partition}] seek 실패: {e}")
                
                # 커밋된 오프셋 조회
                committed = self.consumer.committed(tps)  # 리스트로 반환됨
                committed_offsets = {}
                
                # committed는 tps와 같은 순서로 반환됨
                for i, tp in enumerate(tps):
                    if committed[i] is not None:
                        committed_offsets[tp] = committed[i].offset
                    else:
                        committed_offsets[tp] = low_offsets[tp]
                
                # 각 토픽/파티션의 지연 계산
                self._calculate_partition_lags(tps, committed_offsets)

        except Exception as e:
            monitoring_logger.error(f"Kafka 모니터링 오류: {e}", exc_info=True)

    def _update_partition_info(self):
        """파티션 정보 업데이트"""
        current_time = time.time()
        if not hasattr(self, '_last_partition_update') or \
           current_time - self._last_partition_update > 300:  # 5분마다 갱신
            try:
                cluster_metadata = self.admin_client.list_topics(timeout=10)
                self._partitions = {}  # 모든 토픽의 파티션 정보를 저장

                for topic in self.topics:
                    if topic not in cluster_metadata.topics:
                        monitoring_logger.error(f"토픽을 찾을 수 없음: {topic}")
                        return
                    self._partitions[topic] = cluster_metadata.topics[topic].partitions

                if self._partitions:  # 하나 이상의 유효한 토픽이 존재할 경우에만 업데이트
                    self._last_partition_update = current_time
            except KafkaException as ke:
                monitoring_logger.error(f"토픽 메타��이터 조회 실패: {ke}")
                return

    def _get_committed_offsets(self, tps):
        """커밋된 오프셋 조회"""
        committed_offsets = {}
        max_retries = 3  # 최대 재시도 횟수
        for tp in tps:
            for attempt in range(max_retries):
                try:
                    committed = self.consumer.committed([tp], timeout=5.0)
                    committed_offsets[tp] = committed[0].offset if committed and committed[0] else None
                    break  # 성공적으로 가져오면 루프 종료
                except KafkaException as e:
                    monitoring_logger.error(f"커밋된 오프셋 조회 실패 (시도 {attempt + 1}): {e}")
                    if attempt < max_retries - 1:
                        time.sleep(1)  # 재시도 전 대기
                    else:
                        committed_offsets[tp] = None  # 재시도 실패 시 None으로 설정
        return committed_offsets

    def _calculate_partition_lags(self, tps, committed_offsets):
        """각 파티션의 지연 계산 및 메트릭 업데이트"""
        self.metrics['kafka'] = {}  # 메트릭 초기화
        
        for tp in tps:
            try:
                low, high = self.consumer.get_watermark_offsets(tp)
                committed_offset = committed_offsets.get(tp)

                if committed_offset is None or committed_offset < 0:
                    committed_offset = low  # 커밋된 오프셋이 없거나 잘못된 경우 시작 오프셋 사용

                lag = high - committed_offset
                metric_key = f"{tp.topic}_partition_{tp.partition}_lag"
                self.metrics['kafka'][metric_key] = lag
                
                monitoring_logger.info(
                    f"토픽 {tp.topic} 파티션 {tp.partition} - 지연: {lag} "
                    f"(현재: {committed_offset}, 최신: {high}, 시작: {low})"
                )

            except KafkaError as ke:
                if ke.args[0].code() == KafkaError._UNKNOWN_PARTITION:
                    monitoring_logger.warning(
                        f"토픽 {tp.topic} 파티션 {tp.partition}이 더 이상 존재하지 않습니다."
                    )
                else:
                    monitoring_logger.error(
                        f"토픽 {tp.topic} 파티션 {tp.partition} 지연 계산 오류: {ke}"
                    )

    def monitor_system(self):
        """시스템 리소스 모니터링"""
        try:
            # CPU 사용률
            cpu_usage = psutil.cpu_percent(interval=1)
            
            # 메모리 정보
            memory = psutil.virtual_memory()
            memory_usage = memory.percent
            memory_available = memory.available / (1024 * 1024)  # MB
            
            # 디스크 정보
            disk = psutil.disk_usage('/')
            disk_usage = disk.percent
            disk_available = disk.free / (1024 * 1024 * 1024)  # GB
            
            # 네트워크 정보
            net_io = psutil.net_io_counters()
            
            self.metrics['system'].update({
                'cpu_usage': cpu_usage,
                'memory_usage': memory_usage,
                'memory_available': memory_available,
                'disk_usage': disk_usage,
                'disk_available': disk_available,
                'net_bytes_sent': net_io.bytes_sent,
                'net_bytes_recv': net_io.bytes_recv,
                'net_packets_sent': net_io.packets_sent,
                'net_packets_recv': net_io.packets_recv
            })
            
        except Exception as e:
            print(f"시스템 모니터링 오류: {e}")
            
    def validate_data_format(self, data):
        """데이터 형식 검증"""
        required_fields = ['source', 'timestamp', 'dataType', 'content']
        return all(field in data for field in required_fields)
            
    def validate_data_values(self, data):
        """데이터 값 검증"""
        try:
            # timestamp 형식 검증
            datetime.fromisoformat(data['timestamp'])
            
            # source 검증
            if not isinstance(data['source'], str) or not data['source']:
                return False
                
            # content 검증
            if not isinstance(data['content'], list):
                return False
                
            for item in data['content']:
                # item_code 검증
                if not isinstance(item['item_code'], str) or not item['item_code']:
                    return False
                    
                # current_price 검증
                if not isinstance(item['current_price'], (int, float)) or item['current_price'] < 0:
                    return False
                    
            return True
            
        except (ValueError, KeyError, TypeError):
            return False
            
    def monitor_data_quality(self):
        """데이터 품질 모니터링"""
        try:
            # 이전에 소비된 메시지가 없으면 새로 소비
            messages = self.last_consumed_messages or self._consume_messages()
            self.last_consumed_messages = []  # 캐시 초기화
            
            stats = defaultdict(int)
            data_stats = self._initialize_data_stats()
            
            for message in messages:
                if not self._process_message(message, stats, data_stats):
                    continue
                    
            monitoring_logger.info(f"처리된 통계: {stats}")
            
            if stats['total_count'] > 0:
                self._update_quality_metrics(stats, data_stats)
                
        except Exception as e:
            monitoring_logger.error("데이터 품질 모니터링 오류: %s", e, exc_info=True)
            
    def _initialize_data_stats(self):
        """데이터 통계 초기화"""
        return {
            'avg_price': 0,
            'max_price': float('-inf'),
            'min_price': float('inf'),
            'price_count': 0,
            'unique_sources': set(),
            'unique_items': set(),
            'timestamp_gaps': []
        }
        
    def _process_message(self, message, stats, data_stats):
        """단일 메시지 처리"""
        if message is None or message.error():
            return False
            
        stats['total_count'] += 1
        try:
            data = json.loads(message.value().decode('utf-8'))
            monitoring_logger.info(f"처리 중인 메시지: {data}")
            
            if not self._validate_and_collect_stats(data, stats, data_stats):
                stats['invalid_count'] += 1
                monitoring_logger.warning(f"유효하지 않은 데이터: {data}")
                return False
                
            return True
            
        except Exception as e:
            stats['invalid_count'] += 1
            stats['format_errors'] += 1
            monitoring_logger.error(f"데이터 품질 검사 오류: {e}, 데이터: {message.value()}")
            return False
            
    def _validate_and_collect_stats(self, data, stats, data_stats):
        """데이터 검증 및 통계 수집 통합 처리"""
        try:
            # 필수 필드 검증
            if not all(field in data for field in ['source', 'timestamp', 'content']):
                monitoring_logger.warning(f"필수 필드 누락: {data.keys()}")
                stats['missing_fields'] += 1
                return False
                
            # 타임스탬프 검증
            try:
                current_timestamp = datetime.fromisoformat(data['timestamp'])
            except ValueError as e:
                monitoring_logger.warning(f"잘못된 타임스탬프: {data['timestamp']}, 오류: {e}")
                stats['timestamp_errors'] += 1
                return False
                
            # 데이터 통계 수집
            data_stats['unique_sources'].add(data['source'])
            
            for item in data['content']:
                if not isinstance(item.get('current_price'), (int, float)):
                    monitoring_logger.warning(f"잘못된 가격 데이터: {item}")
                    stats['invalid_values'] += 1
                    return False
                    
                price = item['current_price']
                data_stats['price_count'] += 1
                data_stats['avg_price'] += price
                data_stats['max_price'] = max(data_stats['max_price'], price)
                data_stats['min_price'] = min(data_stats['min_price'], price)
                data_stats['unique_items'].add(item['item_code'])
                
            return True
            
        except Exception as e:
            monitoring_logger.warning(f"데이터 형식 오류: {e}, 데이터: {data}")
            stats['format_errors'] += 1
            return False
            
    def _update_quality_metrics(self, stats, data_stats):
        """통계 계산 및 메트릭 업데이트"""
        if stats['total_count'] > 0:
            if data_stats['price_count'] > 0:
                data_stats['avg_price'] /= data_stats['price_count']
                
            data_stats['unique_sources'] = len(data_stats['unique_sources'])
            data_stats['unique_items'] = len(data_stats['unique_items'])
            
            self.metrics['data_quality'].update({
                'invalid_ratio': (stats['invalid_count'] + stats['format_errors'] + 
                                 stats['missing_fields'] + stats['invalid_values'] + 
                                 stats['timestamp_errors']) / stats['total_count'],
                'total_messages': stats['total_count'],
                'invalid_messages': (stats['invalid_count'] + stats['format_errors'] + 
                                    stats['missing_fields'] + stats['invalid_values'] + 
                                    stats['timestamp_errors']),
                'error_types': {
                    'format': stats['format_errors'],
                    'missing_fields': stats['missing_fields'],
                    'invalid_values': stats['invalid_values'],
                    'timestamp_errors': stats['timestamp_errors']
                },
                'data_stats': data_stats
            })
            
    def start_monitoring(self):
        """모니터링 시작"""
        try:
            if self.monitor_thread and self.monitor_thread.is_alive():
                monitoring_logger.warning("모니터링이 이미 실행 중입니다.")
                return
            
            self.running = True
            self.monitor_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
            self.monitor_thread.start()
            monitoring_logger.info("시스템 모니터링이 시작되었습니다.")
            
        except Exception as e:
            monitoring_logger.error("모니터링 시작 오류: %s", e, exc_info=True)
            raise
            
    def _monitoring_loop(self):
        """모니터링 루프"""
        while self.running:
            try:
                # 시스템 리소스 모니터링
                self.monitor_system()
                
                # Kafka 모니터링
                self.monitor_kafka()
                
                # 데이터 품질 모니터링
                self.monitor_data_quality()
                
                # 알림 체크
                self._check_alerts()
                
                # 메트릭 내보내기
                self.metrics_exporter.export_metrics(self.metrics)
                
                # 30초 대기
                time.sleep(30)
                
            except Exception as e:
                monitoring_logger.error("모니터링 루프 오류: %s", e, exc_info=True)
                if not self.running:
                    break
                time.sleep(30)
                
    def _check_alerts(self):
        """알림 조건 체크"""
        try:
            # CPU 사용률 체크 (80% 이상)
            if self.metrics['system']['cpu_usage'] > 80:
                self.alert_manager.create_alert('high_cpu_usage', 
                    f"CPU 사용률이 {self.metrics['system']['cpu_usage']}%로 높습니다.")
                
            # 메모리 사용률 체크 (90% 이상)
            if self.metrics['system']['memory_usage'] > 90:
                self.alert_manager.create_alert('high_memory_usage',
                    f"메모리 사용률이 {self.metrics['system']['memory_usage']}%로 높습니다.")
                
            # Kafka 지연 체크
            for partition, lag in self.metrics['kafka'].items():
                if lag > 10000:  # 10000개 이상 지연
                    self.alert_manager.create_alert('high_kafka_lag',
                        f"Kafka {partition}의 지연이 {lag}개로 높습니다.")
                    
        except Exception as e:
            monitoring_logger.error("알림 체크 오류: %s", e, exc_info=True)
        
    def stop_monitoring(self):
        """모니터링 중지"""
        try:
            self.running = False
            if self.monitor_thread:
                self.monitor_thread.join(timeout=5)
            monitoring_logger.info("시스템 모니터링 중지되었습니다.")
            
        except Exception as e:
            monitoring_logger.error("모니터링 중지 오류: %s", e, exc_info=True)
        