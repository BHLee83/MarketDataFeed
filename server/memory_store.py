from typing import Dict, List, Optional
from datetime import datetime, timedelta
import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass
import numpy as np

@dataclass
class StatisticsData:
    trd_date: np.ndarray
    trd_time: np.ndarray
    value1: np.ndarray
    value2: np.ndarray
    type: str
    symbol1: str
    symbol2: str

@dataclass
class PriceData:
    trd_date: np.ndarray
    trd_time: np.ndarray
    open: np.ndarray
    high: np.ndarray
    low: np.ndarray
    close: np.ndarray
    volume: np.ndarray
    symbol: str

class MemoryStore:
    def __init__(self):
        self.statistics_data = {}  # {timeframe: {market: {key: data}}}
        self.price_data = {}      # {timeframe: {symbol: data}}
        self.meta_data = []
        self.loading_status = defaultdict(bool)
        self.last_update = defaultdict(datetime.now)
        self.last_load_date = defaultdict(datetime.now)
        self.logger = logging.getLogger('memory_store')
        self.background_task = None

    async def initialize(self, db_manager):
        """비동기 초기화"""
        try:
            # 메타 데이터만 먼저 로드
            self.meta_data = await db_manager.load_marketdata_meta_async()
            
            # 백그라운드 데이터 로딩을 별도 태스크로 시작
            self.background_task = asyncio.create_task(
                self._background_loading(db_manager)
            )
            
            # 백그라운드 태스크 에러 핸들링
            self.background_task.add_done_callback(self._handle_background_task_result)
            
        except Exception as e:
            self.logger.error(f"초기화 중 오류 발생: {e}")
            raise

    def _handle_background_task_result(self, task):
        """백그라운드 태스크 결과 처리"""
        try:
            task.result()  # 에러 체크
        except Exception as e:
            self.logger.error(f"백그라운드 데이터 로딩 중 오류 발생: {e}")

    async def _background_loading(self, db_manager):
        """백그라운드 데이터 로딩"""
        timeframes = ['1d', '30m', '15m', '10m', '5m', '3m', '1m']
        # timeframes = ['1d'] # 임시로 일봉만 로드
        
        # 날짜 범위 설정 (최근 1주)
        total_days = 5
        chunk_size = 1  # 하루 단위로 처리
        
        end_date = datetime.now()
        current_end = end_date
        while total_days > 0:
            # 종료일이 주말(토, 일)이면 평일로 조정
            while current_end.weekday() in (5, 6):  # 5: 토요일, 6: 일요일
                current_end -= timedelta(days=1)

            current_start = current_end - timedelta(days=chunk_size)
            # 시작일이 주말(토, 일)이면 평일로 조정
            while current_start.weekday() in (5, 6):  # 5: 토요일, 6: 일요일
                current_start -= timedelta(days=1)

            # 각 기간에 대해 모든 타임프레임의 데이터를 로드
            for tf in timeframes:
                # self.logger.info(f"{current_start} {tf} 데이터 로드중")
                print(f"{current_start} {tf} 데이터 로드중")
                self.loading_status[tf] = False
                try:
                    await self._load_timeframe_data(
                        db_manager, 
                        tf, 
                        current_start.strftime('%Y%m%d'),
                        current_end.strftime('%Y%m%d')
                    )
                    self.loading_status[tf] = True
                    self.last_update[tf] = datetime.now()
                    self.last_load_date[tf] = current_start
                except Exception as e:
                    self.logger.error(f"{tf} 데이터 로드 중 오류: {e}")

            # 이전 날짜로 이동
            current_end = current_start
            total_days -= chunk_size

        self.logger.info("백그라운드 데이터 로딩 완료")

    async def _load_timeframe_data(self, db_manager, timeframe: str, start_date: str, end_date: str):
        """타임프레임별 데이터 로드"""
        try:
            # 통계 데이터 로드 (await 추가)
            statistics = await db_manager.load_statistics_data(
                f"statistics_price_{timeframe}",
                start_date=start_date,
                end_date=end_date
            )
            
            # 가격 데이터 로드 (await 추가)
            prices = await db_manager.load_price_data(
                f"marketdata_price_{timeframe}",
                start_date=start_date,
                end_date=end_date
            )
            
            # 메모리 저장소에 데이터 저장
            if statistics:
                self._store_statistics_data(timeframe, statistics)
            if prices:
                self._store_price_data(timeframe, prices)
                
            self.last_update[timeframe] = datetime.now()
            
        except Exception as e:
            self.logger.error(f"타임프레임 {timeframe} 데이터 로드 중 오류: {e}")
            raise

    def _store_statistics_data(self, timeframe: str, data: List[Dict]):
        """통계 데이터 저장"""
        try:
            if timeframe not in self.statistics_data:
                self.statistics_data[timeframe] = {}
            
            for item in data:
                market = item['market']
                data_type = item['type']
                key = f"{item['symbol1']}-{item['symbol2']}"
                
                if market not in self.statistics_data[timeframe]:
                    self.statistics_data[timeframe][market] = {}
                if data_type not in self.statistics_data[timeframe][market]:
                    self.statistics_data[timeframe][market][data_type] = {}
                
                # 기존 데이터가 있는 경우 업데이트 또는 추가
                if key in self.statistics_data[timeframe][market][data_type]:
                    existing_data = self.statistics_data[timeframe][market][data_type][key]
                    
                    # 날짜/시간이 같은 데이터가 있는지 확인
                    if timeframe == '1d':
                        mask = existing_data.trd_date == item['trd_date']
                    else:
                        mask = (existing_data.trd_date == item['trd_date']) & (existing_data.trd_time == item.get('trd_time', 0))
                    
                    if np.any(mask):
                        # 기존 데이터 업데이트
                        existing_data.value1[mask] = item['value1']
                        existing_data.value2[mask] = item['value2']
                    else:
                        # 새로운 데이터 추가
                        existing_data.trd_date = np.append(existing_data.trd_date, item['trd_date'])
                        existing_data.trd_time = np.append(existing_data.trd_time, item.get('trd_time', 0))
                        existing_data.value1 = np.append(existing_data.value1, item['value1'])
                        existing_data.value2 = np.append(existing_data.value2, item['value2'])
                else:
                    # 새로운 데이터 생성
                    self.statistics_data[timeframe][market][data_type][key] = StatisticsData(
                        trd_date=np.array([item['trd_date']]),
                        trd_time=np.array([item.get('trd_time', 0)]),
                        value1=np.array([item['value1']]),
                        value2=np.array([item['value2']]),
                        type=data_type,
                        symbol1=item['symbol1'],
                        symbol2=item['symbol2']
                    )
                
        except Exception as e:
            self.logger.error(f"통계 데이터 저장 중 오류: {e}")

    def _store_price_data(self, timeframe: str, data: List[Dict]):
        """가격 데이터 저장"""
        try:
            # timeframe이 없으면 초기화
            if timeframe not in self.price_data:
                self.price_data[timeframe] = {}
            
            # 심볼별로 데이터 그룹화
            for item in data:
                symbol = item['symbol']
                
                if symbol in self.price_data[timeframe]:
                    # 기존 데이터가 있는 경우 병합
                    existing_data = self.price_data[timeframe][symbol]
                    
                    # 새로운 데이터 추가
                    trd_date = np.append(existing_data.trd_date, item['trd_date'])
                    trd_time = np.append(existing_data.trd_time, item.get('trd_time', 0))
                    open_price = np.append(existing_data.open, item['open'])
                    high_price = np.append(existing_data.high, item['high'])
                    low_price = np.append(existing_data.low, item['low'])
                    close_price = np.append(existing_data.close, item['close'])
                    volume = np.append(existing_data.volume, item.get('volume', 0))
                    
                    self.price_data[timeframe][symbol] = PriceData(
                        trd_date=trd_date,
                        trd_time=trd_time,
                        open=open_price,
                        high=high_price,
                        low=low_price,
                        close=close_price,
                        volume=volume,
                        symbol=symbol
                    )
                else:
                    # 새로운 데이터 생성
                    self.price_data[timeframe][symbol] = PriceData(
                        trd_date=np.array([item['trd_date']]),
                        trd_time=np.array([item.get('trd_time', 0)]),
                        open=np.array([item['open']]),
                        high=np.array([item['high']]),
                        low=np.array([item['low']]),
                        close=np.array([item['close']]),
                        volume=np.array([item.get('volume', 0)]),
                        symbol=symbol
                    )
            
        except Exception as e:
            self.logger.error(f"가격 데이터 저장 중 오류: {e}")

    def get_statistics_data(self, data_type: str, timeframe: str, market: str = None, key: str = None, start_date: str = None, end_date: str = None):
        """통계 데이터 조회"""
        try:
            if timeframe not in self.statistics_data or market not in self.statistics_data[timeframe]:
                return None
            
            data = {f'{key}': self.statistics_data[timeframe][market][key]} if key else self.statistics_data[timeframe][market]
            if data_type:
                data = data[data_type]
            
            # 필터링
            if data_type or start_date or end_date:
                filtered_data = {}
                for k, stat_data in data.items():
                    # numpy 마스크를 사용하여 날짜 필터링
                    mask = np.ones(len(stat_data.trd_date), dtype=bool)
                    if start_date:
                        mask &= (stat_data.trd_date.astype(np.int64) >= int(start_date))
                    if end_date:
                        mask &= (stat_data.trd_date.astype(np.int64) <= int(end_date))

                    if np.any(mask):
                        filtered_data[k] = {
                            'trd_date': stat_data.trd_date[mask].tolist(),
                            'trd_time': stat_data.trd_time[mask].tolist(),
                            'value1': stat_data.value1[mask].tolist(),
                            'value2': stat_data.value2[mask].tolist(),
                            'type': stat_data.type,
                            'symbol1': stat_data.symbol1,
                            'symbol2': stat_data.symbol2
                        }
                return filtered_data if filtered_data else None
            
        except Exception as e:
            self.logger.error(f"데이터 조회 중 오류: {e}")
            return None

    def get_loading_status(self):
        """현재 로딩 상태 반환"""
        try:
            return {
                'timeframes': dict(self.loading_status),
                'last_update': {k: v.isoformat() for k, v in self.last_update.items()},
                'last_load': {k: v.date().strftime("%Y-%m-%d") for k, v in self.last_load_date.items()}
            }
        except Exception as e:
            self.logger.error(f"로딩 상태 조회 중 오류: {e}")
            return {
                'timeframes': {},
                'last_update': {},
                'last_load': {}
            }

    def update_realtime_data(self, data: Dict):
        """실시간 데이터 업데이트"""
        try:
            timeframe = data.get('timeframe')
            market = data.get('market')
            type = data.get('type')
            
            if type:
                key = f"{data['symbol1']}-{data['symbol2']}"
                
                if market not in self.statistics_data[timeframe]:
                    self.statistics_data[timeframe][market] = {}
                
                if key not in self.statistics_data[timeframe][market]:
                    # 새로운 StatisticsData 객체 생성
                    self.statistics_data[timeframe][market][key] = StatisticsData(
                        trd_date=np.array([data['trd_date']]),
                        trd_time=np.array([data.get('trd_time', 0)]),
                        value1=np.array([data['value1']]),
                        value2=np.array([data['value2']]),
                        type=data['type'],
                        symbol1=data['symbol1'],
                        symbol2=data['symbol2']
                    )
                else:
                    stat_data = self.statistics_data[timeframe][market][key]
                    # 기존 데이터에서 동일한 시간의 데이터가 있는지 확인
                    if timeframe == '1d':
                        mask = stat_data.trd_date == data['trd_date']
                    else:
                        mask = (stat_data.trd_date == data['trd_date']) & (stat_data.trd_time == data['trd_time'])

                    mask &= (stat_data.type == type)
                    
                    if np.any(mask):
                        # 기존 데이터 업데이트
                        stat_data.value1[mask] = data['value1']
                        stat_data.value2[mask] = data['value2']
                    else:
                        # 새로운 데이터 추가
                        stat_data.trd_date = np.append(stat_data.trd_date, data['trd_date'])
                        stat_data.trd_time = np.append(stat_data.trd_time, data.get('trd_time', 0))
                        stat_data.value1 = np.append(stat_data.value1, data['value1'])
                        stat_data.value2 = np.append(stat_data.value2, data['value2'])
                        
                        # 정렬
                        sort_idx = np.lexsort((stat_data.trd_time, stat_data.trd_date))
                        stat_data.trd_date = stat_data.trd_date[sort_idx]
                        stat_data.trd_time = stat_data.trd_time[sort_idx]
                        stat_data.value1 = stat_data.value1[sort_idx]
                        stat_data.value2 = stat_data.value2[sort_idx]
            
            self.last_update[timeframe] = datetime.now()
        except Exception as e:
            self.logger.error(f"실시간 데이터 업데이트 중 오류: {e}") 