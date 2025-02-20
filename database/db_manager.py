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
                WHERE use_yn = 'Y' 
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
                    WHERE use_yn = 'Y' 
                    ORDER BY market, symbol
                """)
                columns = [col[0].lower() for col in cursor.description]
                rows = cursor.fetchall()
                return [dict(zip(columns, row)) for row in rows]

        return await asyncio.to_thread(_execute_query)


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


    def insert_to_marketdata_price_rt(self, data_list: List[Dict[str, Any]]):
        """실시간 데이터 테이블 일괄 저장"""
        query = """
            INSERT INTO marketdata_price_rt 
            (symbol, timestamp, price, volume)
            VALUES (:symbol, :timestamp, :price, :volume)
        """
        with self.get_cursor() as cursor:
            cursor.executemany(query, data_list)


    async def load_statistics_data(self, table_name: str, max_rows: int = 2500) -> List[Dict]:
        """통계 데이터 로드 - 각 심볼 쌍별로 최근 N개"""
        def _execute_query():
            query = f"""
                WITH ranked_data AS (
                    SELECT 
                        a.*,
                        ROW_NUMBER() OVER (PARTITION BY symbol1, symbol2 
                                         ORDER BY trd_date DESC, trd_time DESC) as rn
                    FROM {table_name} a
                )
                SELECT * FROM ranked_data 
                WHERE rn <= :max_rows
                ORDER BY symbol1, symbol2, trd_date, trd_time
            """
            
            with self.get_cursor() as cursor:
                cursor.execute(query, {"max_rows": max_rows})
                columns = [col[0].lower() for col in cursor.description]
                rows = cursor.fetchall()
                return [dict(zip(columns, row)) for row in rows]

        return await asyncio.to_thread(_execute_query)


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


    async def load_price_data(self, table_name: str, max_rows: int = 2500) -> List[Dict]:
        """가격 데이터 로드 - 각 심볼별로 최근 N개"""
        def _execute_query():
            query = f"""
                WITH ranked_data AS (
                    SELECT 
                        a.*,
                        ROW_NUMBER() OVER (PARTITION BY symbol 
                                         ORDER BY trd_date DESC, trd_time DESC) as rn
                    FROM {table_name} a
                )
                SELECT * FROM ranked_data 
                WHERE rn <= :max_rows
                ORDER BY symbol, trd_date, trd_time
            """
            
            with self.get_cursor() as cursor:
                cursor.execute(query, {"max_rows": max_rows})
                columns = [col[0].lower() for col in cursor.description]
                rows = cursor.fetchall()
                return [dict(zip(columns, row)) for row in rows]

        return await asyncio.to_thread(_execute_query)
    
    def load_close_by_symbol(self, table_name: str, symbol: str, start_date: str = None, end_date: str = None) -> List[Dict]:
        conditions = ["symbol = :symbol"]
        params = {"symbol": symbol}

        if start_date:
            conditions.append("trd_date >= :start_date")
            params["start_date"] = start_date

        if end_date:
            conditions.append("trd_date < :end_date")
            params["end_date"] = end_date

        where_clause = " AND ".join(conditions)
        query = f"""
            SELECT symbol, trd_date, close FROM {table_name} 
            WHERE {where_clause}
            ORDER BY trd_date
        """

        with self.get_cursor() as cursor:
            cursor.execute(query, params)
            columns = [col[0].lower() for col in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
        

    def save_price_data(self, table_name: str, data: List[Dict]):
        """가격 데이터 저장"""
        if not data:
            return
        
        data_tuples = [tuple(item.values()) for item in data]
        query = f"""
            INSERT INTO {table_name} 
            (trd_date, trd_time, symbol, open, high, low, close, 
            open_int, theo_prc, under_lvl, trd_volume, trd_value)
            VALUES 
            (:trd_date, :trd_time, :symbol, :open, :high, :low, :close, 
            :open_int, :theo_prc, :under_lvl, :trd_volume, :trd_value)
        """
        
        with self.get_cursor() as cursor:
            cursor.executemany(query, data_tuples)