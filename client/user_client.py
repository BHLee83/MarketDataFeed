import socket
import threading
import pandas as pd
from datetime import datetime, date

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Config
from database.db_manager import DatabaseManager

class UserClient:
    def __init__(self, host=Config.SERVER_HOST, port=Config.SERVER_PORT):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.host = host
        self.port = port
        self.db_manager = DatabaseManager({
            'dbname': 'market_data',
            'user': 'username',
            'password': 'password',
            'host': 'localhost',
            'port': '5432'
        })
        self.dataset = {}
        
    def connect(self):
        self.socket.connect((self.host, self.port))
        self.load_initial_data()
        
        # 실시간 업데이트를 위한 스레드 시작
        threading.Thread(target=self.receive_updates).start()
        
    def load_initial_data(self):
        self.metadata = self.db_manager.load_metadata()
        self.historical_data = self.db_manager.load_historical_data(
            symbol='AAPL',
            start_date=date(2024, 1, 1),
            end_date=date.today()
        )
        self.temp_data = self.db_manager.load_temp_data(
            symbol='AAPL',
            current_date=date.today()
        )
        
    def request_realtime_data(self, symbol=None):
        request = {
            'type': 'realtime_request',
            'symbol': symbol,
            'timestamp': datetime.now().isoformat()
        }
        self.socket.send(str(request).encode()) 