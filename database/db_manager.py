from typing import Dict, List, Any, Optional
import oracledb
from datetime import datetime, date
import logging
from contextlib import contextmanager
from config import Config
import asyncio

class DatabaseManager:
    def __init__(self):
        """
        Config 클래스에서 DB 연결 정보를 가져와 초기화합니다.
        """
        oracledb.init_oracle_client(lib_dir=Config.ORACLE_CLIENT_PATH)
        self.config = {
            'user': Config.DB_USER,
            'password': Config.DB_PASSWORD,
            'dsn': Config.DB_HOST
        }
        self.conn = None
        self.setup_logging()
        self.pool = None
        self.pool_size = 10

    async def initialize(self):
        """비동기 초기화"""
        await self.setup_pool()

    async def setup_pool(self):
        if not self.pool:
            self.pool = oracledb.create_pool(
                user=Config.DB_USER,
                password=Config.DB_PASSWORD,
                dsn=Config.DB_HOST,
                min=2,
                max=self.pool_size,
                increment=1
            )

        
    def setup_logging(self):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        
    @contextmanager
    def get_connection(self):
        """데이터베이스 연결 제공"""
        if not hasattr(self, 'conn') or self.conn is None:
            self.conn = oracledb.connect(**self.config)
        
        try:
            yield self.conn
        except Exception as e:
            self.logger.error(f"Database connection error: {e}")
            self.conn = None  # 연결 해제
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
    
    def load_marketdata_meta(self):
        """메타데이터 로드"""
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT * FROM marketdata_meta 
                ORDER BY market, symbol
            """)
            columns = [col[0].lower() for col in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
            
    async def load_marketdata_meta_async(self):
        """메타데이터 비동기 로드"""
        def _execute_query():
            with self.get_cursor() as cursor:
                cursor.execute("""
                    SELECT * FROM marketdata_meta 
                    ORDER BY market, symbol
                """)
                columns = [col[0].lower() for col in cursor.description]
                rows = cursor.fetchall()
                return [dict(zip(columns, row)) for row in rows]

        return await asyncio.to_thread(_execute_query)

    def load_marketdata_price(self, table_name, symbol) -> List[Dict]:
        """마켓 가격 데이터 로드"""
        query = f"SELECT * FROM {table_name} WHERE symbol = :symbol ORDER BY trd_date, trd_time"
        with self.get_cursor() as cursor:
            cursor.execute(query, symbol=symbol)
            columns = [col[0].lower() for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def load_marketdata_price_rt(self, start_date: date) -> List[Dict]:
        """실시간 데이터 로드"""
        query = """
            SELECT * FROM marketdata_price_rt 
            WHERE timestamp >= :start_date
            ORDER BY timestamp
        """
        with self.get_cursor() as cursor:
            cursor.execute(query, start_date=start_date)
            columns = [col[0] for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    # def load_historical_data(self, symbol: str, start_date: date, end_date: date) -> List[Dict]:
    #     """과거 데이터 로드"""
    #     query = """
    #         SELECT * FROM market_data 
    #         WHERE symbol = :symbol 
    #         AND date BETWEEN :start_date AND :end_date
    #         ORDER BY date
    #     """
    #     with self.get_cursor() as cursor:
    #         cursor.execute(query, symbol=symbol, start_date=start_date, end_date=end_date)
    #         columns = [col[0] for col in cursor.description]
    #         return [dict(zip(columns, row)) for row in cursor.fetchall()]
    def load_all_marketdata_price(self, timeframes, symbols, batch_size=1000):
        data = {}
        for tf in timeframes:
            table_name = f'marketdata_price_{tf}'
            for i in range(0, len(symbols), batch_size):
                batch = symbols[i:i + batch_size]
                query = f"""
                    SELECT * FROM {table_name}
                    WHERE symbol IN ({','.join(f"'{s}'" for s in batch)}) ORDER BY
                        trd_date,
                        CASE WHEN trd_time = '0000' THEN '2400' ELSE trd_time END
                """
                with self.get_cursor() as cursor:
                    cursor.execute(query)
                    columns = [col[0].lower() for col in cursor.description]
                    results = [dict(zip(columns, row)) for row in cursor.fetchall()]
                for row in results:
                    symbol = row['symbol']
                    if symbol not in data:
                        data[symbol] = {}
                    if tf not in data[symbol]:
                        data[symbol][tf] = []
                    data[symbol][tf].append(row)
        return data


    def load_stat_price(self, table_name: str, start_date: str='19000101') -> List[Dict]:
        """가격 기반 통계 데이터 로드"""
        query = f"""
            SELECT 
                id, trd_date, trd_time, symbol1, symbol2, market, type,
                value1, value2, value3,
                created_at
            FROM {table_name} 
            WHERE trd_date >= :start_date 
            ORDER BY trd_date, trd_time
        """
        with self.get_cursor() as cursor:
            cursor.execute(query, start_date=start_date)
            columns = [col[0].lower() for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        
    
    def get_column_list(self, table_name) -> List[Dict]:
        """대상 테이블 컬럼 리스트 추출"""
        query = f"SELECT * FROM {table_name}"
        with self.get_cursor() as cursor:
            cursor.execute(query)
            columns = [col[0].lower() for col in cursor.description]
            return columns


    def get_last_statistics_date(self, table_name):
        """통계 테이블의 마지막 날짜 조회"""
        query = f"SELECT MAX(trd_date) as last_date FROM {table_name}"
        with self.get_cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchone()
            return result[0] if result else None


    def save_statistics_data(self, table_name: str, data: List[Dict]):
        """통계 데이터 저장"""
        if not data:
            return
        
        data_tuples = [tuple(item.values()) for item in data]
        query = f"""
            INSERT INTO {table_name} 
            (trd_date, trd_time, market, symbol1, symbol2, type, value1, value2, value3)
            VALUES 
            (:trd_date, :trd_time, :market, :symbol1, :symbol2, :type, :value1, :value2, :value3)
        """
        
        with self.get_cursor() as cursor:
            cursor.executemany(query, data_tuples)


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
        """실시간 데이터 테이블 일괄 저장"""
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

    
    async def load_statistics_data_async(self, table_name: str, data_type: str = None, market: str = None, symbol1: str = None, symbol2: str = None,
                                       start_date: str = '19000101', end_date: str = None) -> List[Dict]:
        """통계 데이터 비동기 로드"""
        def _execute_query():
            conditions = ["market = :market"]
            params = {"market": market}
            
            if start_date:
                conditions.append("trd_date >= :start_date")
                params["start_date"] = start_date

            if end_date:
                conditions.append("trd_date < :end_date")
                params["end_date"] = end_date
            
            if data_type:
                conditions.append("type = :data_type")
                params["data_type"] = data_type

            if symbol1:
                conditions.append("symbol1 = :symbol1")
                params["symbol1"] = symbol1

            if symbol2:
                conditions.append("symbol2 = :symbol2")
                params["symbol2"] = symbol2

            where_clause = " AND ".join(conditions)
            query = f"""
                SELECT * FROM {table_name} 
                WHERE {where_clause}
                ORDER BY trd_date, trd_time
            """
            
            with self.get_cursor() as cursor:
                cursor.execute(query, params)
                columns = [col[0].lower() for col in cursor.description]
                rows = cursor.fetchall()
                return [dict(zip(columns, row)) for row in rows]

        return await asyncio.to_thread(_execute_query)

    async def load_latest_statistics_async(self, table_name: str, data_type: str = None, 
                                         start_date: str = None, end_date: str = None) -> List[Dict]:
        """최근 통계 데이터 비동기 로드"""
        def _execute_query():
            conditions = []
            params = {}
            
            if data_type:
                conditions.append("type = :data_type")
                params["data_type"] = data_type
                
            if start_date:
                conditions.append("trd_date >= :start_date")
                params["start_date"] = start_date
            
            if end_date:
                conditions.append("trd_date < :end_date")
                params["end_date"] = end_date

            where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
            query = f"""
                SELECT * FROM {table_name}
                {where_clause}
                ORDER BY trd_date, trd_time
            """
            
            with self.get_cursor() as cursor:
                cursor.execute(query, params)
                columns = [col[0].lower() for col in cursor.description]
                rows = cursor.fetchall()
                return [dict(zip(columns, row)) for row in rows]

        return await asyncio.to_thread(_execute_query)

    async def load_statistics_data(self, table_name: str, start_date: str = None, end_date: str = None) -> List[Dict]:
        """통계 데이터 로드"""
        def _execute_query():
            conditions = []
            params = {}
            
            if start_date:
                conditions.append("trd_date >= :start_date")
                params["start_date"] = start_date
            
            if end_date:
                conditions.append("trd_date < :end_date")
                params["end_date"] = end_date
            
            where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
            query = f"""
                SELECT 
                    trd_date, trd_time, symbol1, symbol2, market, type,
                    value1, value2, value3
                FROM {table_name}
                {where_clause}
                ORDER BY trd_date, trd_time
            """
            
            with self.get_cursor() as cursor:
                cursor.execute(query, params)
                columns = [col[0].lower() for col in cursor.description]
                rows = cursor.fetchall()
                return [dict(zip(columns, row)) for row in rows]

        return await asyncio.to_thread(_execute_query)

    async def load_price_data(self, table_name: str, start_date: str = None, end_date: str = None) -> List[Dict]:
        """가격 데이터 로드"""
        def _execute_query():
            conditions = []
            params = {}
            
            if start_date:
                conditions.append("trd_date >= :start_date")
                params["start_date"] = start_date
            
            if end_date:
                conditions.append("trd_date < :end_date")
                params["end_date"] = end_date
            
            where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
            query = f"""
                SELECT 
                    trd_date, trd_time, symbol, open, high, low, close, 
                    open_int, theo_prc, under_lvl, trd_volume, trd_value
                FROM {table_name}
                {where_clause}
                ORDER BY trd_date, trd_time
            """
            
            with self.get_cursor() as cursor:
                cursor.execute(query, params)
                columns = [col[0].lower() for col in cursor.description]
                rows = cursor.fetchall()
                return [dict(zip(columns, row)) for row in rows]

        return await asyncio.to_thread(_execute_query)