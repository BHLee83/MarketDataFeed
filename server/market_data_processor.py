from datetime import datetime, timedelta
import time
import json
import threading
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
        # self.MAX_CHUNK_SIZE = 100
        self.running = True
        
        # 종목별 데이터 저장소
        self.symbol_data = {}  # {symbol: {'1m': [], '2m': [], ..., '30m': [], '1d': []}}
        
        # 과거 데이터 로드
        if self.load_historical_price_data():
            print("과거 일/분 데이터 로드 및 TIMESERIES 데이터 생성 성공")
            if self.publish_historical_data():  # 과거 데이터 kafka 각 토픽으로 전송
                del self.symbol_data    # 종목별 데이터 저장소 비우기
                print("TIMESERIES 데이터 KAFKA 전송 완료")
        
        # 실시간 데이터 처리를 위한 캐시
        self.tick_cache = {}  # {symbol: [tick_data]}
        
        # 처리 스레드 시작
        self.start_processing_threads()

    def load_historical_price_data(self):
        """과거 일/분 데이터 로드 및 초기화"""
        try:
            # DB에서 모든 종목의 분/일 데이터 조회
            historical_data = self.db_manager.load_marketdata_price()

            # 종목별로 데이터 분류
            for row in historical_data:
                symbol = row['code']
                if symbol not in self.symbol_data:
                    self.symbol_data[symbol] = {
                        '1m': [], '2m': [], '3m': [], '5m': [], 
                        '10m': [], '15m': [], '30m': [], '1d': []
                    }

                # 일봉 데이터 처리
                if row['trd_time'] in ('000000', ''):
                    self.symbol_data[symbol]['1d'].append({
                        'date': row['trd_date'],
                        'open': row['open'],
                        'high': row['high'],
                        'low': row['low'],
                        'close': row['close'],
                        'volume': row['trd_volume']
                    })
                else:
                    # 1분봉 데이터 추가
                    self.symbol_data[symbol]['1m'].append({
                        'date': row['trd_date'],
                        'time': row['trd_time'],
                        'open': row['open'],
                        'high': row['high'],
                        'low': row['low'],
                        'close': row['close'],
                        'volume': row['trd_volume']
                    })

            # 1분봉 데이터로 상위 타임프레임 생성
            for symbol in self.symbol_data:
                self.generate_higher_timeframes(symbol)

            return True

        except Exception as e:
            server_logger.error(f"과거 데이터 로드 중 오류: {e}")
            return False

    def generate_higher_timeframes(self, symbol):
        """상위 타임프레임 데이터 생성"""
        timeframes = {
            '2m': 2, '3m': 3, '5m': 5, '10m': 10,
            '15m': 15, '30m': 30
        }

        one_minute_data = self.symbol_data[symbol]['1m']
        
        for tf, minutes in timeframes.items():
            self.symbol_data[symbol][tf] = self.aggregate_timeframe(
                one_minute_data, minutes
            )

    def aggregate_timeframe(self, minute_data, interval):
        """분봉 데이터를 상위 타임프레임으로 집계"""
        if not minute_data:
            return []

        aggregated = []
        processed_times = set()  # 이미 처리된 시간 추적 (날짜와 시간 포함)

        # 첫 1분봉부터 마지막 1분봉까지 interval 단위로 슬라이딩
        for i in range(0, len(minute_data)):
            current_candle = minute_data[i]
            current_time = int(current_candle['time'])
            current_date = current_candle['date']
            # 타겟 시간을 interval 경계로 맞춤
            target_time = ((current_time - 1) // interval + 1) * interval

            # 날짜와 시간을 함께 저장하여 고유 시간대를 구분
            target_datetime = (current_date, target_time)

            # 이미 처리된 시간대는 건너뜀
            if target_datetime in processed_times:
                continue

            # 같은 날짜이고, 현재 시간 이하인 데이터만 선택
            chunk = [candle for candle in minute_data[i:] 
                    if candle['date'] == current_date and int(candle['time']) < target_time]

            # target_time과 정확히 일치하는 캔들 추가
            exact_match = next((candle for candle in minute_data[i:] 
                            if candle['date'] == current_date and int(candle['time']) == target_time), None)
            if exact_match:
                chunk.append(exact_match)
            
            # chunk에 최소한의 데이터가 있다면 캔들 생성
            if chunk:
                temp_candle = {
                    'date': current_date,
                    'time': str(target_time),
                    'open': chunk[0]['open'],
                    'high': max(candle['high'] for candle in chunk),
                    'low': min(candle['low'] for candle in chunk),
                    'close': chunk[-1]['close'],
                    'volume': sum(candle['volume'] for candle in chunk)
                }
                aggregated.append(temp_candle)
                processed_times.add(target_datetime)

        return aggregated
    
    def publish_historical_data(self):
        """Publish historical data to Kafka topics"""
        for symbol, timeframes in self.symbol_data.items():
            for timeframe, candles in timeframes.items():
                topic = self.set_topic(timeframe)
                for candle in candles:
                    data = {
                        'symbol': symbol,
                        'data': candle
                    }
                    if timeframe != '1d':  # 분봉인 경우 타임프레임 정보 추가
                        data['timeframe'] = timeframe
                    self.kafka_handler.send_data(topic, data)

        return True

    def set_topic(self, timeframe):
        """timeframe별로 topic 네임 설정"""
        if timeframe == '1d':
            return self.topic_day
        elif timeframe.endswith('m'):
            return self.topic_min

    def process_realtime_data(self, market_data):
        """실시간 데이터 처리"""
        for tick in market_data['content']:
            symbol = tick['item_code']
            
            # 종목 초기화
            if symbol not in self.tick_cache:
                self.tick_cache[symbol] = []
            if symbol not in self.symbol_data:
                self.symbol_data[symbol] = {
                    '1m': [], '2m': [], '3m': [], '5m': [],
                    '10m': [], '15m': [], '30m': [], '1d': []
                }

            data = {
                'timestamp': market_data['timestamp'],
                'price': tick['current_price'],
                'volume': tick.get('current_vol', 0)
            }

            # 틱 데이터 캐시에 추가
            self.tick_cache[symbol].append(data)

            # 1분마다 캔들 업데이트
            self.update_candles(symbol)

    def update_candles(self, symbol):
        """캔들 데이터 업데이트"""
        current_time = datetime.now()
        
        # 새로운 분봉 생성 조건 확인
        if self.should_create_new_candle(current_time):
            ticks = self.tick_cache[symbol]
            if not ticks:
                return

            # 새로운 1분봉 생성
            new_candle = self.create_candle_from_ticks(ticks)
            self.symbol_data[symbol]['1m'].append(new_candle)
            # kafka의 minute topic에 전달
            data_to_send = {'symbol': symbol, 'timeframe': '1m', 'data': new_candle}
            self.kafka_handler.send_data(self.topic_min, data_to_send)
            
            # 상위 타임프레임 업데이트
            self.update_higher_timeframes(symbol, new_candle)

            # 일봉 업데이트
            self.update_daily_candle(symbol, new_candle)

            # 틱 캐시 초기화
            self.tick_cache[symbol] = []

    def should_create_new_candle(self, current_time):
        """새로운 캔들을 생성해야 하는지 확인"""
        return current_time.second == 0

    def create_candle_from_ticks(self, ticks):
        """틱 데이터로 캔들 생성"""
        prices = [tick['price'] for tick in ticks]
        volumes = [tick['volume'] for tick in ticks]
        timestamp = ticks[-1]['timestamp']

        return {
            'date': timestamp[:10],
            'time': timestamp[11:16].replace(':', ''),
            'open': prices[0],
            'high': max(prices),
            'low': min(prices),
            'close': prices[-1],
            'volume': sum(volumes)
        }

    def update_higher_timeframes(self, symbol, new_candle):
        """상위 타임프레임 업데이트"""
        timeframes = {
            '2m': 2, '3m': 3, '5m': 5, '10m': 10,
            '15m': 15, '30m': 30
        }

        for tf, minutes in timeframes.items():
            candles = self.symbol_data[symbol][tf]
            current_time = int(new_candle['time'])
            # 완성 시각 계산
            target_time = ((current_time - 1) // minutes) * minutes + minutes
            if len(candles) == 0 or self.is_new_timeframe(new_candle, candles[-1], minutes):
                # 새로운 캔들 시작
                new_candle_copy = new_candle.copy()
                new_candle_copy['time'] = f"{target_time:04}"  # 완성 시각 설정
                candles.append(new_candle_copy)
                data = new_candle_copy
            else:
                # 기존 캔들 업데이트
                last_candle = candles[-1]
                last_candle['high'] = max(last_candle['high'], new_candle['high'])
                last_candle['low'] = min(last_candle['low'], new_candle['low'])
                last_candle['close'] = new_candle['close']
                last_candle['volume'] += new_candle['volume']
                data = last_candle
            # kafka의 minute topic에 전달
            data_to_send = {'symbol': symbol, 'timeframe': tf, 'data': data}
            self.kafka_handler.send_data(self.topic_min, data_to_send)

    def is_new_timeframe(self, new_candle, last_candle, minutes):
        """새로운 타임프레임 여부 확인"""
        new_time = int(new_candle['time'])
        last_time = int(last_candle['time'])

        # HHMM 형식에서 타임프레임의 완성 경계 확인
        new_frame_start = ((new_time - 1) // minutes) * minutes + minutes
        last_frame_start = ((last_time - 1) // minutes) * minutes + minutes

        # 새로운 타임프레임이 시작되었는지 확인
        return new_frame_start > last_frame_start

    def update_daily_candle(self, symbol, new_candle):
        """일봉 데이터 업데이트"""
        daily_candles = self.symbol_data[symbol]['1d']
        
        if not daily_candles or daily_candles[-1]['date'] != new_candle['date']:
            # 새로운 일봉 시작
            daily_candles.append(new_candle.copy())
            data = new_candle
        else:
            # 기존 일봉 업데이트
            last_candle = daily_candles[-1]
            last_candle['high'] = max(last_candle['high'], new_candle['high'])
            last_candle['low'] = min(last_candle['low'], new_candle['low'])
            last_candle['close'] = new_candle['close']
            last_candle['volume'] += new_candle['volume']
            data = last_candle
        # kafka의 minute topic에 전달
        data_to_send = {'symbol': symbol, 'data': data}
        self.kafka_handler.send_data(self.topic_day, data_to_send)

    def get_chart_data(self, symbol, timeframe):
        """차트 데이터 조회"""
        if symbol in self.symbol_data and timeframe in self.symbol_data[symbol]:
            return self.symbol_data[symbol][timeframe]
        return []

    def start_processing_threads(self):
        """데이터 처리 스레드 시작"""
        self.threads = {
            'time_series': threading.Thread(target=self.process_time_series, daemon=True)
            # 'statistics': threading.Thread(target=self.process_statistics, daemon=True),
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
                messages = self.consumer.consume(num_messages=100, timeout=1.0)
                for msg in messages:
                    if msg.error():
                        continue
                        
                    market_data = json.loads(msg.value().decode('utf-8'))
                    # self.update_time_series(market_data)
                    self.process_realtime_data(market_data)
                    
                # 주기적으로 캐시 데이터를 DB에 저장
                # self.store_time_series_data()
                    
            except Exception as e:
                print(f"시계열 데이터 처리 오류: {e}")
                time.sleep(1)

    # def update_time_series(self, market_data):
    #     """시계열 데이터 업데이트"""
    #     timestamp = datetime.fromisoformat(market_data['timestamp'])
        
    #     # 체결 데이터 업데이트
    #     self.time_series_cache['tick'].append(market_data)
        
    #     # 분봉 데이터 업데이트
    #     minute_key = timestamp.strftime('%Y%m%d%H%M')
    #     if minute_key not in self.time_series_cache['minute']:
    #         self.time_series_cache['minute'][minute_key] = []
    #     self.time_series_cache['minute'][minute_key].append(market_data)
        
    #     # 일봉 데이터 업데이트
    #     daily_key = timestamp.strftime('%Y%m%d')
    #     if daily_key not in self.time_series_cache['daily']:
    #         self.time_series_cache['daily'][daily_key] = []
    #     self.time_series_cache['daily'][daily_key].append(market_data)

    #     # 분봉 데이터 생성
    #     self.generate_minute_bars(market_data)

    # def generate_minute_bars(self, market_data):
    #     """1분봉~30분봉 데이터 생성"""
    #     timestamp = datetime.fromisoformat(market_data['timestamp'])
    #     minute = timestamp.minute

    #     for interval in range(1, 31):
    #         if minute % interval == 0:
    #             key = timestamp.strftime('%Y%m%d%H') + f"{minute // interval * interval:02d}"
    #             if key not in self.minute_bars_cache[interval]:
    #                 self.minute_bars_cache[interval][key] = []
    #             self.minute_bars_cache[interval][key].append(market_data)
                
    def process_statistics(self):
        """통계 데이터 처리"""
        while self.running:
            try:
                self.calculate_volume_profile()
                self.calculate_price_levels()
                self.calculate_moving_averages()
                time.sleep(1)  # 1초마다 통계 업데이트
                
            except Exception as e:
                print(f"통계 데이터 처리 오류: {e}")
                time.sleep(1)
                
    def process_chart_data(self):
        """차트 데이터 생성"""
        while self.running:
            try:
                self.generate_chart_data()
                time.sleep(0.5)  # 0.5초마다 차트 데이터 업데이트
                
            except Exception as e:
                print(f"차트 데이터 처리 오류: {e}")
                time.sleep(1)
                
    # def _chunk_data(self, data):
    #     """대용량 데이터를 작은 청크로 분할"""
    #     for i in range(0, len(data), self.MAX_CHUNK_SIZE):
    #         yield data[i:i + self.MAX_CHUNK_SIZE]

    # def publish_processed_data(self):
    #     """생성된 실시간 데이터를 Kafka의 각 토픽에 발행"""
    #     while self.running:
    #         try:
    #             # 시계열 데이터 발행 (청크 단위)
    #             if self.time_series_cache['tick']:
    #                 for tick_chunk in self._chunk_data(self.time_series_cache['tick']):
    #                     processed_tick_data = {
    #                         'type': 'tick_data',
    #                         'data': tick_chunk
    #                     }
    #                     self.kafka_handler.send_data(
    #                         topic=self.topic_tick, 
    #                         data=processed_tick_data
    #                     )
    #                 # 발행 후 캐시 초기화
    #                 self.time_series_cache['tick'] = []

    #             # 분봉 데이터 발행 (청크 단위)
    #             for minute_key, minute_data in list(self.time_series_cache['minute'].items()):
    #                 for minute_chunk in self._chunk_data(minute_data):
    #                     processed_minute_data = {
    #                         'type': 'minute_data',
    #                         'key': minute_key,
    #                         'data': minute_chunk
    #                     }
    #                     self.kafka_handler.send_data(
    #                         topic=self.topic, 
    #                         data=processed_minute_data
    #                     )
    #                 # 발행 후 해당 분봉 데이터 삭제
    #                 del self.time_series_cache['minute'][minute_key]

    #             # 통계 데이터 발행 (청크 단위)
    #             if self.statistics_cache:
    #                 processed_stats_data = {
    #                     'type': 'statistics_data',
    #                     'data': self.statistics_cache
    #                 }
    #                 self.kafka_handler.send_data(
    #                     topic=self.topic, 
    #                     data=processed_stats_data
    #                 )

    #             # 차트 데이터 발행 (필요시 청크 처리)
    #             if self.chart_cache:
    #                 processed_chart_data = {
    #                     'type': 'chart_data',
    #                     'data': self.chart_cache
    #                 }
    #                 self.kafka_handler.send_data(
    #                     topic=self.topic, 
    #                     data=processed_chart_data
    #                 )

    #             # 일정 간격으로 발행
    #             time.sleep(5)  # 5초마다 처리된 데이터 발행

    #         except Exception as e:
    #             server_logger.error(f"처리된 데이터 발행 오류: {e}")
    #             time.sleep(1)

    def store_time_series_data(self):
        """시계열 데이터 DB 저장"""
        current_time = datetime.now()
        
        # 전일 데이터 저장 (새벽 5시)
        if current_time.hour == 5 and current_time.minute == 0:
            previous_day = (current_time - timedelta(days=1)).strftime('%Y%m%d')
            if previous_day in self.time_series_cache['daily']:
                self.db_manager.store_daily_data(
                    self.time_series_cache['daily'][previous_day]
                )
                del self.time_series_cache['daily'][previous_day]

    # 아직 구현되지 않은 메서드들 (플레이스홀더)
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