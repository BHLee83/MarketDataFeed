from collections import defaultdict
import json
from datetime import datetime
import requests
from config import Config

class DataProcessor:
    def __init__(self, db_manager, kafka_handler, consumer):
        self.db_manager = db_manager
        self.kafka_handler = kafka_handler
        self.consumer = consumer
        self.metadata = []
        self.dataset = []
        self.dsHistorical = []
        self.dsToday = []
        self.index = defaultdict(list)

    # def process_real_time_data(self, market_data):
    #     """실시간 데이터 처리"""
    #     try:
    #         processed_data = {
    #             'source': str(market_data.source),
    #             'timestamp': str(market_data.timestamp),
    #             'data_type': str(market_data.dataType),
    #             'content': [{
    #                 'item_code': str(item.itemCode),
    #                 'current_price': float(item.currentPrice)
    #             } for item in market_data.content]
    #         }
    #         self.dataset.append(processed_data)
    #         print(f"실시간 데이터가 처리되었습니다. 데이터 수: {len(self.dataset)}")
    #     except Exception as e:
    #         print(f"실시간 데이터 처리 오류: {e}")

    # def process_batch(self):
    #     """배치 데이터 처리"""
    #     # 배치 처리 로직 구현
    #     pass

    # def complete_dataset(self):
    #     """전체 데이터셋 완성"""
    #     self.load_metadata()  # 메타데이터 로드
    #     self.load_today_data()  # 오늘자 데이터 로드
        
    #     # 메시지 큐로부터 실시간 데이터 수신 및 처리
    #     while self.running:
    #         messages = self.consumer.consume(num_messages=100, timeout=1.0)
    #         for msg in messages:
    #             if not msg.error():
    #                 self.process_real_time_data(json.loads(msg.value().decode('utf-8')))

    def load_initial_data(self):
        """DB에서 과거 데이터 및 당일 실시간 데이터 로드"""
        self.metadata = self.db_manager.load_marketdata_meta()  # 메타 데이터 로드

        # self.load_historical_data()  # 과거 데이터 로드
        self.load_today_data() # 당일 데이터 로드

        # TODO: 데이터셋 결합, 중복 제거 등 처리
        # self.dataset = _combine_data(self.dsHistorical, today_data)
        # self.dataset = self.remove_duplicates(self.dataset)  # 중복 제거


    def load_historical_data(self):
        dictDS_price = {
            'data_type': 'price', 
            'time_series': '', 
            'content': [{
                'item_code': '', 
                'timestamp': '', 
                'open': None, 
                'high': None, 
                'low': None, 
                'close': None,
                'volume': None
            }]
        }

        dictDS_price['time_series'] = 'daily'
        dictDS_price['content'] = self.db_manager.load_marketdata_price_daily()
        self.dsHistorical.extend(dictDS_price)
        # dictDS_price['time_series'] = '1m'
        # dictDS_price['content'] = self.db_manager.load_marketdata_price_1m()
        # self.dsHistorical.append(dictDS_price)
        # dictDS_price['time_series'] = 'tick'
        # dictDS_price['content'] = self.db_manager.load_marketdata_price_tick()
        # self.dsHistorical.append(dictDS_price)

        print("마켓 일별 데이터 로드 완료: {}건".format(len(self.dsHistorical)))
        self.dataset = self.dsHistorical

    def load_today_data(self):
        today = datetime.today().date()
        stored_data = self.db_manager.load_marketdata_price_rt(today)    # 오늘자 실시간 데이터 적재분 로드
        if stored_data:
            # TODO: 오늘자 데이터(틱)로 분봉 등 생성 처리
            # TODO: dataset 형식에 맞게 정리
            # self.dataset.extend(stored_data)
            print(f"오늘자 실시간 데이터 로드 완료: {len(stored_data)}건")
        else:
            print("현재 시점에서 로드할 오늘자 데이터 적재분은 없습니다")
        # missing_data = self.request_missing_data()  # 오늘자 실시간 데이터 미적재분 요청
        # if missing_data:
        #     # self.dataset.extend(missing_data)
        #     print(f"메인 서버로부터 실시간 데이터 데이터 수신 완료: {len(missing_data)}건")
        # else:
        #     print("메인 서버로부터 수신한 실시간 데이터는 없습니다")

    def request_missing_data(self):
        """메인 서버에 누락된 데이터 요청"""
        try:
            response = requests.get("http://localhost:5000/api/missing_data")
            if response.status_code == 200:
                missing_data = response.json()
                return missing_data
            else:
                print(f"데이터 요청 실패: {response.status_code} - {response.text}")
                return []
        except Exception as e:
            print(f"누락된 데이터 요청 중 오류 발생: {e}")
            return []
    
    def remove_duplicates(self, data):
        """중복된 데이터 제거"""
        seen = set()
        unique_data = []
        
        for item in data:
            identifier = (item['content'][0]['item_code'], item['timestamp'])  # 고유 식별자
            if identifier not in seen:
                seen.add(identifier)
                unique_data.append(item)
        
        return unique_data

    def process_kafka_data(self, market_data):
        """Kafka로부터 수신된 실시간 데이터 처리"""
        try:
            received_data = {
                'source': market_data['source'],
                'timestamp': market_data['timestamp'],
                'data_type': market_data['data_type'],
                'content': [{
                    'item_code': item['item_code'],
                    'current_price': item['current_price']
                } for item in market_data['content']]
            }

            # 데이터셋에 추가하고 중복 제거
            self.dataset.append(received_data)

            # Kafka로 가공된 데이터 전송
            self.kafka_handler.send_data(received_data, topic=Config.KAFKA_TOPICS['PROCESSED_DATA'])

        except Exception as e:
            print(f"[Processing Server] 실시간 데이터 수신 오류: {e}")

    def manage_data_flow(self):
        """전체 데이터 흐름 관리"""
        messages = self.consumer.consume(num_messages=100, timeout=1.0)
        for msg in messages:
            if not msg.error():
                market_data = json.loads(msg.value().decode('utf-8'))
                self.process_kafka_data(market_data)
        
        # self.save_to_db()  # 통합된 데이터셋을 DB에 저장
