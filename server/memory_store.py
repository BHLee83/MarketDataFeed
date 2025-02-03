from typing import Dict, List, Optional
from datetime import datetime, timedelta
import asyncio
import logging
from collections import defaultdict

class MemoryStore:
    def __init__(self):
        self.statistics_data = defaultdict(lambda: defaultdict(dict))  # timeframe -> market -> key -> data
        self.price_data = defaultdict(lambda: defaultdict(list))      # timeframe -> symbol -> data
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
        
        # 날짜 범위 설정 (최근 1개월)
        total_days = 8
        chunk_size = 1  # 하루 단위로 처리
        
        end_date = datetime.now()
        for day_offset in range(0, total_days, chunk_size):
            current_end = end_date - timedelta(days=day_offset)
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
            for item in data:
                market = item['market']
                key = f"{item['symbol1']}-{item['symbol2']}"
                if key not in self.statistics_data[timeframe][market]:
                    self.statistics_data[timeframe][market][key] = []
                self.statistics_data[timeframe][market][key].append(item)
        except Exception as e:
            self.logger.error(f"통계 데이터 저장 중 오류: {e}")

    def _store_price_data(self, timeframe: str, data: List[Dict]):
        """가격 데이터 저장"""
        try:
            for item in data:
                symbol = item['symbol']
                if symbol not in self.price_data[timeframe]:
                    self.price_data[timeframe][symbol] = []
                self.price_data[timeframe][symbol].append(item)
        except Exception as e:
            self.logger.error(f"가격 데이터 저장 중 오류: {e}")

    def get_statistics_data(self, data_type: str, timeframe: str, market: str = None, key: str = None, start_date: str = None, end_date: str = None):
        """통계 데이터 조회"""
        try:
            if market and market in self.statistics_data[timeframe]:
                data = {f'{key}': self.statistics_data[timeframe][market][key]} if key else self.statistics_data[timeframe][market]
                
                # 필터링
                if data_type or start_date or end_date:
                    filtered_data = {}
                    for k, items in data.items():
                        values = []
                        for item in items:
                            item_date = str(item['trd_date'])
                            if (item['type'] == data_type) and (not start_date or item_date >= start_date) and \
                            (not end_date or item_date <= end_date):
                                values.append(item)
                        if values:
                            filtered_data[k] = values
                    return filtered_data if filtered_data else None
                
                return data
            return None
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
            
            if type == 'spread' or type.startswith('corr'):
                key = f"{data['symbol1']}-{data['symbol2']}"
                
                # 해당 키가 없으면 리스트 초기화
                if key not in self.statistics_data[timeframe][market]:
                    self.statistics_data[timeframe][market][key] = []
                
                # 기존 데이터 중 동일한 시간의 데이터가 있는지 확인
                existing_data = None
                for idx, item in enumerate(self.statistics_data[timeframe][market][key]):
                    if timeframe == '1d':
                        # 일봉은 날짜만 비교
                        if item['trd_date'] == data['trd_date']:
                            existing_data = (idx, item)
                            break
                    else:
                        # 분봉은 날짜와 시간 모두 비교
                        if item['trd_date'] == data['trd_date'] and item['trd_time'] == data['trd_time']:
                            existing_data = (idx, item)
                            break
                
                if existing_data:
                    # 동일한 시간의 데이터가 있으면 업데이트
                    idx, _ = existing_data
                    self.statistics_data[timeframe][market][key][idx] = data
                else:
                    # 새로운 데이터면 추가
                    self.statistics_data[timeframe][market][key].append(data)
                
                # 타임프레임에 따라 다른 정렬 기준 적용
                # if timeframe == '1d':
                #     # 일봉은 날짜만으로 정렬
                #     self.statistics_data[timeframe][market][key].sort(
                #         key=lambda x: int(x['trd_date'])
                #     )
                # else:
                #     # 분봉은 날짜와 시간으로 정렬
                #     self.statistics_data[timeframe][market][key].sort(
                #         key=lambda x: (int(x['trd_date']), int(x['trd_time'].strip() or '0'))
                #     )
                
            else:
                # 가격 데이터도 동일한 로직 적용
                symbol = data.get('symbol')
                if symbol not in self.price_data[timeframe]:
                    self.price_data[timeframe][symbol] = []
                
                existing_data = None
                for idx, item in enumerate(self.price_data[timeframe][symbol]):
                    if timeframe == '1d':
                        if item['trd_date'] == data['trd_date']:
                            existing_data = (idx, item)
                            break
                    else:
                        if item['trd_date'] == data['trd_date'] and item['trd_time'] == data['trd_time']:
                            existing_data = (idx, item)
                            break
                    
                if existing_data:
                    idx, _ = existing_data
                    self.price_data[timeframe][symbol][idx] = data
                else:
                    self.price_data[timeframe][symbol].append(data)
                
                # 타임프레임에 따라 다른 정렬 기준 적용
                # if timeframe == '1d':
                #     self.price_data[timeframe][symbol].sort(
                #         key=lambda x: int(x['trd_date'])
                #     )
                # else:
                #     self.price_data[timeframe][symbol].sort(
                #         key=lambda x: (int(x['trd_date']), int(x['trd_time'].strip() or '0'))
                #     )
                
            self.last_update[timeframe] = datetime.now()
        except Exception as e:
            self.logger.error(f"실시간 데이터 업데이트 중 오류: {e}") 