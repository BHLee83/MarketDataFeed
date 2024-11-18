from prometheus_client import start_http_server, Gauge, Counter
import threading

class MetricsExporter:
    def __init__(self):
        # Prometheus 메트릭 서버 시작 (포트 8000)
        start_http_server(8000)
        
        # 시스템 메트릭 게이지 초기화
        self.system_metrics = {
            'cpu_usage': Gauge('system_cpu_usage', 'CPU usage percentage'),
            'memory_usage': Gauge('system_memory_usage', 'Memory usage percentage'),
            'disk_usage': Gauge('system_disk_usage', 'Disk usage percentage'),
            'net_bytes_sent': Counter('system_network_bytes_sent', 'Network bytes sent'),
            'net_bytes_recv': Counter('system_network_bytes_received', 'Network bytes received')
        }
        
        # Kafka 지연 메트릭 게이지 초기화 - 단일 게이지로 변경
        self.kafka_lag_metric = Gauge('kafka_consumer_lag', 'Kafka consumer lag', ['partition'])
        
        # 데이터 품질 메트릭 게이지 초기화
        self.data_quality_metrics = {
            'invalid_ratio': Gauge('data_quality_invalid_ratio', 'Invalid data ratio'),
            'total_messages': Counter('data_quality_total_messages', 'Total messages processed'),
            'invalid_messages': Counter('data_quality_invalid_messages', 'Invalid messages count'),
            'format_errors': Counter('data_quality_format_errors', 'Format error count'),
            'missing_fields': Counter('data_quality_missing_fields', 'Missing fields count'),
            'invalid_values': Counter('data_quality_invalid_values', 'Invalid values count')
        }
        
    def export_metrics(self, metrics):
        """메트릭 내보내기"""
        try:
            # 시스템 메트릭 업데이트
            for key, gauge in self.system_metrics.items():
                if key in metrics['system']:
                    if isinstance(gauge, Counter):
                        # Counter의 경우 증가분만 추가
                        current_value = gauge._value.get()
                        increment = metrics['system'][key] - current_value
                        if increment > 0:
                            gauge.inc(increment)
                    else:
                        gauge.set(metrics['system'][key])
            
            # Kafka 지연 메트릭 업데이트
            for partition, lag in metrics['kafka'].items():
                partition_id = partition.split('_')[1].split('_')[0]  # partition_0_lag에서 0만 추출
                self.kafka_lag_metric.labels(partition=f'partition-{partition_id}').set(lag)
            
            # 데이터 품질 메트릭 업데이트
            if 'data_quality' in metrics and metrics['data_quality']:
                quality = metrics['data_quality']
                for key, gauge in self.data_quality_metrics.items():
                    if key in quality:
                        if isinstance(gauge, Counter):
                            # Counter의 경우 현재 값과의 차이만큼 증가
                            gauge.inc(quality[key])
                        else:
                            gauge.set(quality[key])
                
                # 에러 타입별 카운터 업데이트
                if 'error_types' in quality:
                    for error_type, count in quality['error_types'].items():
                        metric_name = f'data_quality_error_{error_type}'
                        if metric_name not in self.data_quality_metrics:
                            self.data_quality_metrics[metric_name] = Counter(
                                metric_name, 
                                f'Count of {error_type} errors'
                            )
                        self.data_quality_metrics[metric_name].inc(count)
                    
        except Exception as e:
            print(f"메트릭 내보내기 오류: {e}")