import logging
import os

def setup_logger(name, log_file=None):
    """로거 설정"""
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 콘솔 핸들러
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # 파일 핸들러 (지정된 경우)
    if log_file:
        # 로그 디렉토리 생성
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger

# 공통 로거 인스턴스 생성
server_logger = setup_logger('server', 'logs/server.log')
kafka_logger = setup_logger('kafka', 'logs/kafka.log')
monitoring_logger = setup_logger('monitoring', 'logs/monitoring.log') 