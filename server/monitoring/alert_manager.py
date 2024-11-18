from datetime import datetime
import threading
import time
from config import Config

class AlertManager:
    def __init__(self):
        self.alerts = []
        self.alert_thread = None
        self.running = True
        self.thresholds = {
            'kafka': {
                'max_lag': 1000,  # 최대 지연 임계값
                'lag_warning': 500  # 경고 지연 임계값
            },
            'system': {
                'cpu_warning': 80,  # CPU 사용률 경고 임계값
                'memory_warning': 80,  # 메모리 사용률 경고 임계값
                'disk_warning': 80  # 디스크 사용률 경고 임계값
            },
            'data_quality': {
                'invalid_ratio_warning': 0.1,  # 데이터 오류율 경고 임계값
                'max_timestamp_gap': 300,  # 최대 타임스탬프 간격 (초)
                'min_messages': 100  # 최소 메시지 수
            }
        }
        
    def check_thresholds(self, metrics):
        """임계값 체크 및 알림 생성"""
        try:
            # Kafka 지연 체크
            for partition, lag in metrics['kafka'].items():
                if lag > self.thresholds['kafka']['max_lag']:
                    self.create_alert('critical', f'Kafka {partition} 지연이 심각함: {lag}')
                elif lag > self.thresholds['kafka']['lag_warning']:
                    self.create_alert('warning', f'Kafka {partition} 지연 발생: {lag}')
                    
            # 시스템 리소스 체크
            system = metrics['system']
            if system.get('cpu_usage', 0) > self.thresholds['system']['cpu_warning']:
                self.create_alert('warning', f'CPU 사용률 높음: {system["cpu_usage"]}%')
                
            if system.get('memory_usage', 0) > self.thresholds['system']['memory_warning']:
                self.create_alert('warning', f'메모리 사용률 높음: {system["memory_usage"]}%')
                
            if system.get('disk_usage', 0) > self.thresholds['system']['disk_warning']:
                self.create_alert('warning', f'디스크 사용률 높음: {system["disk_usage"]}%')
                
            # 데이터 품질 체크
            data_quality = metrics['data_quality']
            if data_quality.get('total_messages', 0) >= self.thresholds['data_quality']['min_messages']:
                if data_quality.get('invalid_ratio', 0) > self.thresholds['data_quality']['invalid_ratio_warning']:
                    self.create_alert('warning', 
                        f'데이터 오류율 높음: {data_quality["invalid_ratio"]*100:.2f}% '
                        f'(총 {data_quality["total_messages"]}개 중 {data_quality["invalid_messages"]}개)')
                
                for error_type, count in data_quality.get('error_types', {}).items():
                    if count > 0:
                        self.create_alert('info', f'데이터 오류 발생: {error_type} - {count}건')
                        
        except Exception as e:
            print(f"임계값 체크 중 오류 발생: {e}")
    
    def create_alert(self, severity, message):
        """알림 생성"""
        alert = {
            'timestamp': datetime.now(),
            'severity': severity,
            'message': message
        }
        self.alerts.append(alert)
        self.notify_alert(alert)
        
    def notify_alert(self, alert):
        """알림 전송"""
        print(f"[{alert['timestamp']}] {alert['severity'].upper()}: {alert['message']}")
        
    def start(self):
        """알림 관리자 시작"""
        def alert_loop():
            while self.running:
                try:
                    # 오래된 알림 정리 (24시간 이상)
                    current_time = datetime.now()
                    self.alerts = [alert for alert in self.alerts 
                                 if (current_time - alert['timestamp']).total_seconds() < 86400]
                    time.sleep(60)
                except Exception as e:
                    print(f"알림 관리자 오류: {e}")
                    if not self.running:
                        break
                    time.sleep(60)
                
        self.alert_thread = threading.Thread(target=alert_loop)
        self.alert_thread.daemon = True
        self.alert_thread.start()
        
    def stop(self):
        """알림 관리자 종료"""
        self.running = False
        if self.alert_thread:
            self.alert_thread.join(timeout=5) 