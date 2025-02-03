from datetime import datetime, timedelta
import time
import json
import threading
import numpy as np

import pandas as pd

from config import Config
from utils.logger import server_logger

class MarketDataProcessor:
    def __init__(self, db_manager, kafka_handler, consumer):
        self.db_manager = db_manager
        self.kafka_handler = kafka_handler
        self.consumer = consumer
        # self.topic_tick = Config.KAFKA_TOPICS['RAW_MARKET_DATA_TICK']
        self.topic_min = Config.KAFKA_TOPICS['RAW_MARKET_DATA_MINUTE']
        self.topic_day = Config.KAFKA_TOPICS['RAW_MARKET_DATA_DAY']
        self.topic_statistics = Config.KAFKA_TOPICS['STATISTICS_DATA']
        # self.MAX_CHUNK_SIZE = 100
        self.running = True

        # 세팅값
        self.timeframes = ['1m', '3m', '5m', '10m', '15m', '30m', '1d']
        self.stat_pair_types = ['spread', 'correlation']    # 페어 계산 종류
        self.corr_periods = [30, 120]    # 상관계수 계산 기간값 (30일, 120일)

        # 데이터 저장소
        self.symbol_meta = None # {symbol, country_code, market, name, data_type, instrument_type, source, ...}

        self.symbol_data = {}  # {symbol: {'1m': [], '3m': [], ..., '30m': [], '1d': []}}
        self.stat_data = {tf: {} for tf in self.timeframes}   # { '1m': {pair: [statistics_data]}, '3m': {}, ..., '30m': {}, '1d': {} }
        
        self.market_symbols = {}    # {market: [symbol1, symbol2, ...]}
        self.last_stat_dates = {}   # {tf: last_stat_date}

        # 실시간 데이터 처리를 위한 캐시
        self.tick_cache = {}  # {symbol: [tick_data]}
        self.stat_cache = {tf: {} for tf in self.timeframes}

        # 과거 데이터 로드 및 통계 계산
        if self.load_hist_price():   # 과거 가격 데이터 로드
            server_logger.info(f"과거 가격 데이터 로드 완료: {len(self.symbol_data)} 종목")
            # if self.publish_hist_price():   # MQ 전송
            #     server_logger.info("과거 가격 데이터 KAFKA 전송 완료")
            # if self.load_hist_stat_price():   # 과거 통계 데이터 로드
            #     server_logger.info("과거 통계 데이터 로드 완료")
            #     if self.publish_stat_price():   # MQ 전송
            #         server_logger.info("과거 통계값 KAFKA 전송 완료")
            if self.calculate_latest_pair():  # 최신 데이터에 대한 통계값 계산(일반적으로 전일자)
                for tf in self.timeframes:  # last_stat_date 값들 미리 로드
                    self.last_stat_dates[tf] = self.db_manager.get_last_statistics_date(f"statistics_price_{tf}")
                
                server_logger.info("최근 통계값 계산 완료")
                # server_logger.info("과거 데이터 프로세스 완료")
        
        # 처리 스레드 시작
        self.start_processing_threads()


    def load_hist_price(self):
        """과거 가격 데이터 로드"""
        try:
            # 메타데이터에서 종목코드 목록 조회
            self.symbol_meta = self.db_manager.load_marketdata_meta()
            symbols = [item['symbol'] for item in self.symbol_meta]
            if not symbols:
                server_logger.info("메타 테이블에 종목코드 데이터가 없습니다.")
                return False
            
            # market별로 심볼 그룹화
            markets = list(set([item['market'] for item in self.symbol_meta])) # ['EQ', 'IR', 'FX', 'CM', 'ECO', ...]
            for market in markets:
                self.market_symbols[market] = [item['symbol'] for item in self.symbol_meta if item['market'] == market]

            server_logger.info("과거 가격 데이터 로드중...")
            self.symbol_data = self.db_manager.load_all_marketdata_price(self.timeframes, symbols)

            return True

        except Exception as e:
            server_logger.error(f"가격 데이터 로드 중 오류: {e}")
            return False
        
    
    def load_hist_stat_price(self):
        """과거 통계 데이터 DB에서 로드"""
        try:
            server_logger.info("과거 통계 데이터 로드중...")
            # 각 타임프레임별로 독립적으로 통계 데이터 로드
            for tf in self.timeframes:
                table_name = f'statistics_price_{tf}'
                # stat_price_data = self.db_manager.load_stat_price(table_name)
                stat_price_data = self.db_manager.load_stat_price(table_name, '20250110')   # 테스트용
                self._update_stat_data(tf, stat_price_data)
            
            return True
        except Exception as e:
            server_logger.error(f"과거 통계 데이터 로드 실패: {e}")
            return False
        

    def publish_hist_price(self):
        """과거 데이터를 Kafka 토픽으로 발행"""
        for symbol, timeframes in self.symbol_data.items():
            for tf, candles in timeframes.items():
                topic = self.set_topic('price', tf)
                for candle in candles:
                    data = {
                        'symbol': symbol,
                        'data': candle
                    }
                    if tf != '1d':  # 분봉인 경우 타임프레임 정보 추가
                        data['timeframe'] = tf
                    self.kafka_handler.send_data(topic, data)

        return True
    
    
    def publish_stat_price(self):
        """과거 통계 데이터를 Kafka 토픽으로 발행"""
        topic = self.set_topic('statistics')
        for tf, pairs in self.stat_data.items():
            for pair, stats in pairs.items():
                for data in stats:
                    # 스프레드 데이터 전송
                    spread_data = {
                        'type': 'spread',
                        'timeframe': tf,
                        'symbol': pair,
                        'market': data['market'],
                        'symbol1': data['symbol1'],
                        'symbol2': data['symbol2'],
                        'value': data['spread'],
                        'trd_date': data['trd_date'],
                        'trd_time': data['trd_time']
                    }
                    self.kafka_handler.send_data(topic, spread_data)
                    
                    # 상관계수 데이터 전송
                    for period in self.corr_periods:
                        corr_data = {
                            'type': 'correlation',
                            'timeframe': tf,
                            'symbol': pair,
                            'market': data['market'],
                            'symbol1': data['symbol1'],
                            'symbol2': data['symbol2'],
                            'period': period,
                            'value': data[f'corr_{period}'],
                            'trd_date': data['trd_date'],
                            'trd_time': data['trd_time']
                        }
                        self.kafka_handler.send_data(topic, corr_data)

        return True


    def set_topic(self, type, tf=None):
        """데이터 종류, 타임프레임에 따라 topic 네임 설정"""
        if type == 'price':
            if tf == '1d':
                return self.topic_day
            elif tf.endswith('m'):
                return self.topic_min

        elif type == 'statistics':
            return self.topic_statistics


    def process_realtime_data(self, market_data):
        """실시간 데이터 처리"""
        for tick in market_data['content']:
            symbol = tick['item_code']
            
            # 종목 초기화
            if symbol not in self.tick_cache:
                self.tick_cache[symbol] = []
            if symbol not in self.symbol_data:
                self.symbol_data[symbol] = {tf: [] for tf in self.timeframes}

            data = {
                'timestamp': market_data['timestamp'],
                'price': tick['current_price'],
                'volume': tick.get('current_vol', 0)
            }

            # 틱 데이터 캐시에 추가
            self.tick_cache[symbol].append(data)

            # 새로운 분봉 생성 조건 확인
            if self.should_create_new_candle(symbol):
                self.update_candles(symbol) # 캔들 업데이트
                self.tick_cache[symbol] = []    # 틱 캐시 초기화

                self.calculate_statistics(symbol)   # 통계값 계산


    def should_create_new_candle(self, symbol):
        """분봉 생성 조건 확인"""
        if not self.tick_cache or len(self.tick_cache[symbol]) < 2:
            return False
        
        # 분봉 생성 조건: 틱타임 기준 1분마다
        last_tick = self.tick_cache[symbol][-1]
        second_last_tick = self.tick_cache[symbol][-2]
        last_minute = datetime.fromisoformat(last_tick['timestamp']).minute
        second_last_minute = datetime.fromisoformat(second_last_tick['timestamp']).minute
        
        return last_minute != second_last_minute
    

    def update_candles(self, symbol):
        """캔들 데이터 업데이트"""
        new_candle = {}
        
        # 캔들 데이터 채움
        ticks = self.tick_cache[symbol][:-1]
        # if ticks:
        dates = [datetime.fromisoformat(tick['timestamp']).strftime('%Y%m%d') for tick in ticks]
        times = [datetime.fromisoformat(tick['timestamp']).strftime('%H%M') for tick in ticks]
        prices = [tick['price'] for tick in ticks]
        volumes = [tick['volume'] for tick in ticks]
        new_candle.update({
            'trd_date': dates[-1],
            'trd_time': times[-1],
            'open': prices[0],
            'high': max(prices),
            'low': min(prices),
            'close': prices[-1],
            'trd_volume': sum(volumes)
        })
        # else:
        #     # 틱 데이터가 없는 경우 직전 캔들의 종가를 사용
        #     prev_candles = self.symbol_data[symbol]['1m']
        #     if prev_candles:
        #         date_obj = datetime.strptime(prev_candles[-1]['trd_date'], '%Y%m%d').date()
        #         time_obj = datetime.strptime(prev_candles[-1]['trd_time'], '%H%M').time()
        #         dummy_datetime = datetime.combine(date_obj, time_obj)
        #         new_datetime = dummy_datetime + timedelta(minutes=1)
        #         last_price = prev_candles[-1]['close']
        #         new_candle.update({
        #             'trd_date': new_datetime.strftime('%Y%m%d'),
        #             'trd_time': new_datetime.strftime('%H%M'),
        #             'open': last_price,
        #             'high': last_price,
        #             'low': last_price,
        #             'close': last_price,
        #             'trd_volume': 0
        #         })
        
        if '1m' not in self.symbol_data[symbol]:
            self.symbol_data[symbol]['1m'] = []
        self.symbol_data[symbol]['1m'].append(new_candle)
        
        # kafka 전송 및 상위 타임프레임 업데이트
        data_to_send = {'symbol': symbol, 'timeframe': '1m', 'data': new_candle}
        self.kafka_handler.send_data(self.topic_min, data_to_send)
        
        self.update_higher_timeframes(symbol, new_candle)
        self.update_daily_candle(symbol, new_candle)


    def create_candle_from_ticks(self, ticks):
        """틱 데이터로 캔들 생성"""
        prices = [tick['price'] for tick in ticks]
        volumes = [tick['volume'] for tick in ticks]
        timestamp = ticks[-1]['timestamp']

        return {
            'trd_date': timestamp[:10],
            'trd_time': timestamp[11:16].replace(':', ''),
            'open': prices[0],
            'high': max(prices),
            'low': min(prices),
            'close': prices[-1],
            'volume': sum(volumes)
        }


    def update_higher_timeframes(self, symbol, new_candle):
        """상위 타임프레임 업데이트"""
        for tf in self.timeframes:
            if tf == '1m' or tf == '1d':
                continue
            minutes = int(tf[:-1])
            if tf not in self.symbol_data[symbol]:
                self.symbol_data[symbol][tf] = []
            candles = self.symbol_data[symbol][tf]
            current_time = int(new_candle['trd_time'])  # 'HHMM' 형식
            # 완성 시각 계산
            currnet_minutes = (current_time // 100) * 60 + (current_time % 100)
            target_minutes = ((currnet_minutes - 1) // minutes) * minutes + minutes
            target_time = f"{target_minutes // 60:02}{target_minutes % 60:02}"  # 'HHMM' 형식
            last_time = int(candles[-1]['trd_time']) if candles else 0
            last_minutes = (last_time // 100) * 60 + (last_time % 100)
            if len(candles) == 0 or (target_minutes > last_minutes):
                # 새로운 캔들 시작
                new_candle_copy = new_candle.copy()
                new_candle_copy['trd_time'] = target_time   # 완성 시각 설정
                candles.append(new_candle_copy)
            else:
                # 기존 캔들 업데이트
                last_candle = candles[-1]
                last_candle['high'] = max(last_candle['high'], new_candle['high'])
                last_candle['low'] = min(last_candle['low'], new_candle['low'])
                last_candle['close'] = new_candle['close']
                last_candle['trd_volume'] += new_candle['trd_volume']
                if currnet_minutes == target_minutes:  # 캔들이 완성되었을 때
                    # kafka의 minute topic에 전달
                    data_to_send = {'symbol': symbol, 'timeframe': tf, 'data': last_candle}
                    self.kafka_handler.send_data(self.topic_min, data_to_send)
                    print("kafka의 minute topic에 전달: ", data_to_send)


    # def is_new_timeframe(self, new_time, last_candle, minutes):
    #     """새로운 타임프레임 여부 확인"""
    #     last_time = int(last_candle['trd_time'])

    #     # HHMM 형식에서 타임프레임의 완성 경계 확인
    #     last_frame_start = ((last_time - 1) // minutes) * minutes + minutes

    #     # 새로운 타임프레임이 시작되었는지 확인
    #     return new_time > last_frame_start


    def update_daily_candle(self, symbol, new_candle):
        """일봉 데이터 업데이트"""
        daily_candles = self.symbol_data[symbol]['1d']
        
        if len(daily_candles) == 0 or daily_candles[-1]['trd_date'] != new_candle['trd_date']:
            # 새로운 일봉 시작
            daily_candles.append(new_candle.copy())
            # kafka의 minute topic에 전달
            data_to_send = {'symbol': symbol, 'data': daily_candles[-1]}
            print("kafka의 day topic에 전달: ", data_to_send)
            self.kafka_handler.send_data(self.topic_day, data_to_send)
        else:
            # 기존 일봉 업데이트
            last_candle = daily_candles[-1]
            last_candle['high'] = max(last_candle['high'], new_candle['high'])
            last_candle['low'] = min(last_candle['low'], new_candle['low'])
            last_candle['close'] = new_candle['close']
            last_candle['trd_volume'] += new_candle['trd_volume']


    def get_chart_data(self, symbol, timeframe):
        """차트 데이터 조회"""
        if symbol in self.symbol_data and timeframe in self.symbol_data[symbol]:
            return self.symbol_data[symbol][timeframe]
        return []


    def start_processing_threads(self):
        """데이터 처리 스레드 시작"""
        self.threads = {
            'time_series': threading.Thread(target=self.process_time_series, daemon=True),
            # 'statistics': threading.Thread(target=self.process_statistics, daemon=True)
            # 'chart_data': threading.Thread(target=self.process_chart_data, daemon=True),
            # 'publish_processed_data': threading.Thread(target=self.publish_processed_data, daemon=True)
        }
        
        for thread in self.threads.values():
            thread.start()


    def stop(self):
        """데이터 처리 스레드 종료"""
        server_logger.info("\n서버를 종료합니다...")
        
        self.running = False
        for thread in self.threads.values():
            thread.join()


    def process_time_series(self):
        """시계열 데이터 처리"""
        while self.running:
            try:
                messages = self.consumer.consume(timeout=1.0)
                for msg in messages:
                    if msg.error():
                        continue
                        
                    market_data = json.loads(msg.value().decode('utf-8'))
                    self.process_realtime_data(market_data)
                    
            except Exception as e:
                server_logger.error(f"시계열 데이터 처리 오류: {e}")
                time.sleep(1)


    def calculate_statistics(self, symbol):
        """통계 계산"""
        self.calculate_pair(symbol)
        # self.calculate_volume_profile()
        # self.calculate_price_levels()
        # self.calculate_moving_averages()


    def _correlation(self, x, y):
        """두 시계열 데이터의 상관계수 계산 (최적화 버전)"""
        try:
            if len(x) != len(y):
                return 0
            
            n = len(x)
            if n < 2:
                return 0
            
            x = np.array(x, dtype=np.float64)
            y = np.array(y, dtype=np.float64)
            
            # 평균 계산
            mean_x = x.mean()
            mean_y = y.mean()
            
            # 편차 계산
            xm = x - mean_x
            ym = y - mean_y
            
            # 공분산과 표준편차 계산 (벡터화 연산)
            covariance = (xm * ym).sum()
            std_x = np.sqrt((xm ** 2).sum())
            std_y = np.sqrt((ym ** 2).sum())
            
            if std_x == 0 or std_y == 0:
                return 0
            
            # 피어슨 상관계수 계산
            correlation = covariance / (std_x * std_y)
            
            return max(min(correlation, 1.0), -1.0)
            
        except Exception as e:
            server_logger.error(f"상관계수 계산 중 오류 발생: {e}")
            return 0


    def _calculate_pair_stat(self, market, symbol_i, symbol_j, tf, last_stat_date=None, last_stat_time=None):
        """두 시계열간 통계 데이터 계산"""
        data_i = self.symbol_data.get(symbol_i, {}).get(tf)
        data_j = self.symbol_data.get(symbol_j, {}).get(tf)
        if not data_i or not data_j:
            return []

        # 두 심볼의 공통 시계열 찾기
        if tf == '1d':
            dates_i = {d['trd_date']: idx for idx, d in enumerate(data_i)}
            dates_j = {d['trd_date']: idx for idx, d in enumerate(data_j)}
            common_dates = sorted(set(dates_i.keys()) & set(dates_j.keys()))
            calc_dates = [d for d in common_dates if d > last_stat_date] if last_stat_date else common_dates
        else:
            dates_i = {(d['trd_date'], d['trd_time']): idx for idx, d in enumerate(data_i)}
            dates_j = {(d['trd_date'], d['trd_time']): idx for idx, d in enumerate(data_j)}
            common_dates = sorted(set(dates_i.keys()) & set(dates_j.keys()))
            if last_stat_time:
                calc_dates = [
                    d for d in common_dates 
                    if (d[0] > last_stat_date) or (d[0] == last_stat_date and d[1] > last_stat_time)
                ] if last_stat_date else common_dates
            else:
                calc_dates = [
                    d for d in common_dates 
                    if d[0] > last_stat_date
                ] if last_stat_date else common_dates

        if not calc_dates:
            return []

        market_results = []
        for type in self.stat_pair_types:
            for calc_date in calc_dates:
                idx_i = dates_i[calc_date]
                idx_j = dates_j[calc_date]

                # 기본 데이터 준비
                common = {
                    'trd_date': calc_date[0] if tf != '1d' else calc_date,
                    'trd_time': calc_date[1] if tf != '1d' else ' ',
                    'market': market,
                    'symbol1': symbol_i,
                    'symbol2': symbol_j,
                }

                # 스프레드 계산 및 저장
                if type == 'spread':
                    value = data_i[idx_i]['close'] - data_j[idx_j]['close']
                    market_results.append({
                        **common,
                        'type': type,
                        'value1': value,
                        'value2': None,
                        'value3': None
                    })

                elif type == 'correlation':
                    # 상관계수 계산 및 저장
                    for period in self.corr_periods:
                        if idx_i >= period and idx_j >= period:
                            close_i = [data_i[k]['close'] for k in range(idx_i - period + 1, idx_i + 1)]
                            close_j = [data_j[k]['close'] for k in range(idx_j - period + 1, idx_j + 1)]
                            value = self._correlation(close_i, close_j)
                            
                            market_results.append({
                                **common,
                                'type': f'corr_{period}',
                                'value1': value,
                                'value2': None,
                                'value3': None
                            })

        return market_results
    

    def calculate_pair(self, updated_symbol):
        """실시간 데이터에 대한 통계값 계산 (변경된 심볼 기준)"""
        try:
            # updated_symbol이 속한 마켓 찾기
            target_market = None
            for market, symbols in self.market_symbols.items():
                if updated_symbol in symbols:
                    target_market = market
                    break
            
            if not target_market:
                server_logger.warning(f"Symbol not found in any market: {updated_symbol}")
                return

            topic = self.set_topic('statistics')

            for tf in self.timeframes:
                if tf == '1d':
                    continue

                market_results = []
                last_stat_date = self.last_stat_dates[tf]
                
                # 해당 마켓에 대해서만 처리
                symbols = self.market_symbols[target_market]
                symbol_pairs = []
                symbol_idx = symbols.index(updated_symbol)
                
                # 앞쪽 심볼들과의 페어
                symbol_pairs.extend([(symbols[i], updated_symbol) for i in range(symbol_idx)])
                # 뒤쪽 심볼들과의 페어
                symbol_pairs.extend([(updated_symbol, symbols[i]) for i in range(symbol_idx + 1, len(symbols))])

                # 페어별 통계 계산
                for symbol_i, symbol_j in symbol_pairs:
                    pair = f"{symbol_i}-{symbol_j}"
                    last_stat_time = None

                    if pair in self.stat_data[tf].keys():
                        last_record = max(self.stat_data[tf][pair], key=lambda x: (x.get('trd_date', ''), x.get('trd_time', '')))
                        last_stat_date, last_stat_time = last_record['trd_date'], last_record['trd_time']

                    market_results_pair = self._calculate_pair_stat(target_market, symbol_i, symbol_j, tf, last_stat_date, last_stat_time)
                    if market_results_pair:
                        market_results.extend(market_results_pair)
                        # Kafka 전송을 배치로 처리
                        processed_data = [{
                            'timeframe': tf,
                            **data
                        } for data in market_results_pair]
                        self.kafka_handler.send_batch(topic, processed_data)
                        print(f"통계값 계산 및 전송: {processed_data}")

                # 캐시 업데이트를 배치로 처리
                if market_results:
                    self._update_stat_data(tf, market_results)
                    self._update_stat_cache(tf, market_results)

        except Exception as e:
            server_logger.error(f"실시간 통계값 계산 중 오류: {e}")


    def calculate_latest_pair(self):
        """DB기준 최신 마켓데이터에 대한 통계값 계산"""
        try:
            for tf in self.timeframes:
                last_stat_date = self.db_manager.get_last_statistics_date(f"statistics_price_{tf}")
                if not last_stat_date:
                    last_stat_date = "19000101"

                for market, symbols in self.market_symbols.items():
                    market_results = []
                    for i, symbol_i in enumerate(symbols):
                        for j, symbol_j in enumerate(symbols[i+1:], i+1):
                            market_results_pair = self._calculate_pair_stat(market, symbol_i, symbol_j, tf, last_stat_date)
                            if market_results_pair:
                                market_results.extend(market_results_pair)
                                # DB 저장
                                self.db_manager.save_statistics_data(f"statistics_price_{tf}", market_results_pair)

                if market_results:
                    self._update_stat_data(tf, market_results)
                    self._update_stat_cache(tf, market_results)

            return True
        except Exception as e:
            server_logger.error(f"과거 통계값 계산 중 오류: {e}")
            return False


    def _update_stat_cache(self, tf, data):
        """통계 캐시 업데이트"""
        for item in data:
            pair_key = f"{item['symbol1']}-{item['symbol2']}"
            if pair_key not in self.stat_cache[tf]:
                self.stat_cache[tf][pair_key] = []
            
            filtered_data = {key: value for key, value in item.items() if key not in ['id', 'created_at']}
            self.stat_cache[tf][pair_key].append(filtered_data)

    def _update_stat_data(self, tf, data):
        """통계 데이터 업데이트"""
        for item in data:
            pair_key = f"{item['symbol1']}-{item['symbol2']}"
            if pair_key not in self.stat_data[tf]:
                self.stat_data[tf][pair_key] = []
            
            filtered_data = {key: value for key, value in item.items() if key not in ['id', 'created_at']}
            self.stat_data[tf][pair_key].append(filtered_data)


    def calculate_volume_profile(self):
        """거래량 프로파일 계산"""
        pass
    
    def calculate_price_levels(self):
        """가격대별 거래량 계산"""
        pass
    
    def calculate_moving_averages(self):
        """이동평균 계산"""
        pass
    
    def generate_chart_data(self):
        """차트 데이터 생성"""
        pass