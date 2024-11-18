from typing import Dict, List, Any, Optional
import cx_Oracle
from datetime import datetime, date
import logging
from contextlib import contextmanager
from config import Config

class DatabaseManager:
    def __init__(self):
        """
        Config 클래스에서 DB 연결 정보를 가져와 초기화합니다.
        """
        self.config = {
            'user': Config.DB_USER,
            'password': Config.DB_PASSWORD,
            'dsn': Config.DB_HOST
        }
        self.conn = None
        self.setup_logging()
        
    def setup_logging(self):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        
    @contextmanager
    def get_connection(self):
        """데이터베이스 연결을 컨텍스트 매니저로 제공"""
        try:
            if not self.conn:
                self.conn = cx_Oracle.connect(**self.config)
            yield self.conn
        except Exception as e:
            self.logger.error(f"Database connection error: {e}")
            raise
        
    @contextmanager
    def get_cursor(self):
        """커서를 컨텍스트 매니저로 제공"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                yield cursor
                conn.commit()
            except Exception as e:
                conn.rollback()
                self.logger.error(f"Database operation error: {e}")
                raise
            finally:
                cursor.close()

    def load_marketdata_meta(self) -> List[Dict]:
        """마켓 메타데이터 로드"""
        query = "SELECT * FROM marketdata_meta"
        with self.get_cursor() as cursor:
            cursor.execute(query)
            columns = [col[0] for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def load_historical_data(self, symbol: str, start_date: date, end_date: date) -> List[Dict]:
        """과거 데이터 로드"""
        query = """
            SELECT * FROM market_data 
            WHERE symbol = :symbol 
            AND date BETWEEN :start_date AND :end_date
            ORDER BY date
        """
        with self.get_cursor() as cursor:
            cursor.execute(query, symbol=symbol, start_date=start_date, end_date=end_date)
            columns = [col[0] for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def load_temp_data(self, symbol: str, current_date: date) -> List[Dict]:
        """임시 테이블 데이터 로드"""
        query = """
            SELECT * FROM temp_market_data 
            WHERE symbol = :symbol 
            AND TRUNC(timestamp) = :current_date
            ORDER BY timestamp
        """
        with self.get_cursor() as cursor:
            cursor.execute(query, symbol=symbol, current_date=current_date)
            columns = [col[0] for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def insert_to_marketdata_price_rt(self, data_list: List[Dict[str, Any]]):
        """실시간 데이터 임시 테이블 일괄 저장"""
        query = """
            INSERT INTO marketdata_price_rt 
            (symbol, timestamp, price, volume)
            VALUES (:symbol, :timestamp, :price, :volume)
        """
        with self.get_cursor() as cursor:
            cursor.executemany(query, data_list)

    def migrate_temp_to_permanent(self, target_date: date):
        """임시 데이터를 영구 테이블로 이관"""
        queries = [
            # 일봉 데이터 집계 및 이관
            """
            INSERT INTO market_data (symbol, date, open, high, low, close, volume)
            SELECT 
                symbol,
                TRUNC(timestamp) as date,
                FIRST_VALUE(price) OVER (PARTITION BY symbol, TRUNC(timestamp) ORDER BY timestamp) as open,
                MAX(price) as high,
                MIN(price) as low,
                LAST_VALUE(price) OVER (PARTITION BY symbol, TRUNC(timestamp) ORDER BY timestamp 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as close,
                SUM(volume) as volume
            FROM temp_market_data
            WHERE TRUNC(timestamp) = :target_date
            GROUP BY symbol, TRUNC(timestamp)
            """,
            # 이관 완료된 임시 데이터 삭제
            """
            DELETE FROM temp_market_data
            WHERE TRUNC(timestamp) = :target_date
            """
        ]
        
        with self.get_cursor() as cursor:
            for query in queries:
                cursor.execute(query, target_date=target_date)

    def cleanup_old_temp_data(self, days_to_keep: int = 7):
        """오래된 임시 데이터 정리"""
        query = """
            DELETE FROM temp_market_data
            WHERE timestamp < SYSDATE - :days_to_keep
        """
        with self.get_cursor() as cursor:
            cursor.execute(query, days_to_keep=days_to_keep)

    def get_latest_price(self, symbol: str) -> Optional[Dict]:
        """특정 심볼의 최신 가격 정보 조회"""
        query = """
            SELECT * FROM temp_market_data
            WHERE symbol = :symbol
            AND ROWNUM = 1
            ORDER BY timestamp DESC
        """
        with self.get_cursor() as cursor:
            cursor.execute(query, symbol=symbol)
            columns = [col[0] for col in cursor.description]
            row = cursor.fetchone()
            return dict(zip(columns, row)) if row else None

    def batch_insert_temp_data(self, data_list: List[Dict[str, Any]]):
        """배치로 임시 데이터 저장"""
        query = """
            INSERT INTO temp_market_data 
            (symbol, timestamp, price, volume, source)
            VALUES (:symbol, :timestamp, :price, :volume, :source)
        """
        
        with self.get_cursor() as cursor:
            cursor.executemany(query, data_list)