import time
from functools import wraps
from config import Config

class RetryHandler:
    def __init__(self, max_retries=3, delay=1, backoff=2):
        self.max_retries = max_retries
        self.delay = delay
        self.backoff = backoff
        
    def retry_with_backoff(self, operation):
        @wraps(operation)
        def wrapper(*args, **kwargs):
            retries = 0
            current_delay = self.delay
            
            while retries < self.max_retries:
                try:
                    return operation(*args, **kwargs)
                except Exception as e:
                    retries += 1
                    if retries == self.max_retries:
                        raise e
                    
                    print(f"작업 실패 ({retries}/{self.max_retries}), {current_delay}초 후 재시도...")
                    time.sleep(current_delay)
                    current_delay *= self.backoff
            
        return wrapper 