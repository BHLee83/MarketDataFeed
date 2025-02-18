import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Config
from fastapi import FastAPI, HTTPException, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, FileResponse
from confluent_kafka import Consumer, KafkaError
import json
import asyncio
from typing import Dict, List
import threading
from queue import Queue
from prometheus_fastapi_instrumentator import Instrumentator
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from database.db_manager import DatabaseManager
from datetime import datetime, timedelta
import uuid
from memory_store import MemoryStore

app = FastAPI()

# CORS 설정 추가
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 실제 운영 환경에서는 구체적인 도메인을 지정하세요
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 데이터 큐 설정
data_queues = {}  # WebSocket 연결별 데이터 큐

# Kafka Consumer 설정
def create_kafka_consumer(topic: str, from_beginning: bool = True):
    consumer = Consumer({
        'bootstrap.servers': Config.KAFKA_BOOTSTRAP_SERVERS,
        'group.id': f'web_consumer_{topic}_{datetime.now().timestamp()}',  # 유니크한 그룹 ID
        'auto.offset.reset': 'earliest' if from_beginning else 'latest'
    })
    consumer.subscribe([topic])
    return consumer

# Kafka 데이터 처리 스레드
def kafka_consumer_thread(timeframe: str, queue: Queue, symbol: str, interval: str = None):
    topic = {
        'tick': Config.KAFKA_TOPICS['RAW_MARKET_DATA'],
        'minute': Config.KAFKA_TOPICS['RAW_MARKET_DATA_MINUTE'],
        'day': Config.KAFKA_TOPICS['RAW_MARKET_DATA_DAY']
    }.get(timeframe)
    
    consumer = create_kafka_consumer(topic, from_beginning=True)
    
    # 과거 데이터 로드
    messages = []
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                break
            if msg.error():
                continue
            
            data = json.loads(msg.value().decode('utf-8'))
            
            # 분봉 데이터의 경우 요청한 interval만 필터링
            if timeframe == 'minute' and interval:
                if 'timeframe' not in data or data['timeframe'] != interval:
                    continue
            
            if data['symbol'] == symbol:
                messages.append(data)
            
            # 충분한 과거 데이터를 로드했다면 중단
            # if len(messages) >= 1000:  # 예: 최대 1000개 캔들
            #     break
                
        # 과거 데이터를 한번에 전송
        if messages:
            queue.put({
                'type': 'historical_data',
                'data': messages
            })
            print(f"Loaded historical data of symbol {symbol}: {len(messages)} candles")
            
        # 실시간 데이터 처리
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                continue
            
            data = json.loads(msg.value().decode('utf-8'))
            
            # 분봉 데이터의 경우 요청한 interval만 필터링
            if timeframe == 'minute' and interval:
                if 'timeframe' not in data or data['timeframe'] != interval:
                    continue
                    
            if data['symbol'] == symbol:
                queue.put({
                    'type': 'market_data',
                    'data': data
                })
                
    except Exception as e:
        print(f"Kafka consumer error: {e}")
    finally:
        consumer.close()

# WebSocket 연결 관리
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, List[WebSocket]] = {
            'tick': [],
            'minute': [],
            'daily': []
        }

    async def connect(self, websocket: WebSocket, data_type: str):
        await websocket.accept()
        self.active_connections[data_type].append(websocket)

    def disconnect(self, websocket: WebSocket, data_type: str):
        self.active_connections[data_type].remove(websocket)

manager = ConnectionManager()

# Prometheus 메트릭스 수집기 설정
instrumentator = Instrumentator()

# 정적 파일 서비스 설정
app.mount("/static", StaticFiles(directory="server/static"), name="static")

@app.get("/")
async def read_root():
    return FileResponse("server/static/index.html")

@app.get("/api/symbols")
async def get_symbols():
    try:
        # 메모리 저장소에서 메타데이터 사용
        symbols = [item['symbol'] for item in memory_store.meta_data]
        if not symbols:  # 메모리에 없는 경우 DB에서 로드
            meta = await db_manager.load_marketdata_meta_async()  # await 추가
            symbols = [item['symbol'] for item in meta]
        return {"symbols": symbols}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.websocket("/ws/chart/{timeframe}/{symbol}")
async def websocket_endpoint(websocket: WebSocket, timeframe: str, symbol: str, interval: str = None):
    try:
        await websocket.accept()
        print(f"New WebSocket connection: timeframe={timeframe}, symbol={symbol}, interval={interval}")
        
        queue = Queue()
        data_queues[websocket] = queue
        current_symbol = symbol
        
        # Kafka consumer 스레드 시작
        kafka_thread = threading.Thread(
            target=kafka_consumer_thread,
            args=(timeframe, queue, current_symbol, interval),
            daemon=True
        )
        kafka_thread.start()
        
        while True:
            try:
                # Kafka 데이터 처리
                while not queue.empty():
                    market_data = queue.get_nowait()
                    print(f"Sending data to client: {market_data}")  # 디버그용
                    await websocket.send_json(market_data)
                
                # 클라이언트 메시지 처리
                try:
                    data = await asyncio.wait_for(websocket.receive_json(), timeout=0.1)
                    if data.get('type') == 'symbol_change':
                        current_symbol = data['symbol']
                        # 심볼 변경 시 새로운 Kafka consumer 스레드 시작
                        kafka_thread = threading.Thread(
                            target=kafka_consumer_thread,
                            args=(timeframe, queue, current_symbol, interval),
                            daemon=True
                        )
                        kafka_thread.start()
                except asyncio.TimeoutError:
                    pass  # 타임아웃은 정상적인 상황
                    
            except WebSocketDisconnect:
                print("WebSocket disconnected")
                break
            except Exception as e:
                print(f"WebSocket error: {e}")
                break
                
    finally:
        if websocket in data_queues:
            del data_queues[websocket]

# DB 매니저 인스턴스
db_manager = DatabaseManager()
memory_store = MemoryStore()

@app.on_event("startup")
async def startup_event():
    """앱 시작 시 초기화"""
    await db_manager.initialize()  # DB 매니저 초기화
    await memory_store.initialize(db_manager)  # 메모리 저장소 초기화

def _create_consumer():
    """Kafka Consumer 생성"""
    return Consumer({
        'bootstrap.servers': Config.KAFKA_BOOTSTRAP_SERVERS,
        'group.id': f'web_consumer_{uuid.uuid4()}',
        'auto.offset.reset': 'latest',  # 실시간 데이터만 구독
        'enable.auto.commit': True
    })

@app.get("/api/loading-status")
async def get_loading_status():
    """데이터 로딩 상태 확인 API"""
    try:
        return JSONResponse(memory_store.get_loading_status())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/statistics/{data_type}/{timeframe}/{market}")
async def get_statistics(data_type: str, timeframe: str, market: str = None, key: str = None, start: str = None, end: str = None):
    """저장소에서 통계 데이터 조회"""
    try:
        if memory_store.loading_status[timeframe]:  # 메모리에 데이터 로딩 중인 경우
            data = memory_store.get_statistics_data(data_type, timeframe, market, key, start, end)  # 메모리에서 통계 데이터 가져오고
        if data is None:    # 메모리에 없는 경우 DB 확인
            table_name = f"statistics_price_{timeframe}"
            symbol1, symbol2 = key.split('-') if key and '-' in key else (None, None)
            data = await db_manager.load_statistics_data_async(table_name, data_type, market, symbol1, symbol2, start, end)
            memory_store._store_statistics_data(timeframe, data)
            data = memory_store.get_statistics_data(data_type, timeframe, market, key, start, end)
            
        # if data is None:
        #     raise HTTPException(status_code=404, detail="Data not found")
            
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/admin/memory-store-status")
async def get_admin_memory_store_status():
    """관리자용 메모리 저장소 상태 확인"""
    status = {
        'loading_status': memory_store.get_loading_status(),
        'statistics_data_count': {
            tf: {
                market: len(data) 
                for market, data in memory_store.statistics_data[tf].items()
            }
            for tf in memory_store.loading_status.keys()
        },
        'price_data_count': {
            tf: len(memory_store.price_data[tf])
            for tf in memory_store.loading_status.keys()
        },
        'memory_usage': {
            'statistics': sys.getsizeof(memory_store.statistics_data),
            'price': sys.getsizeof(memory_store.price_data)
        }
    }
    return status

@app.get("/api/last-date/{timeframe}")
async def getLastDate(timeframe: str):
    table_name = f"marketdata_price_{timeframe}"
    data = db_manager.get_last_statistics_date(table_name)

    return data

@app.websocket("/ws/price/{timeframe}/{symbol}")
async def price_websocket_endpoint(websocket: WebSocket, timeframe: str, symbol: str):
    """가격 데이터 웹소켓 엔드포인트"""
    await websocket.accept()
    try:
        # 초기 데이터 전송 (메모리 저장소에서)
        market = next((item['market'] for item in memory_store.meta_data if item['symbol'] == symbol), None)
        if market and memory_store.loading_status[timeframe]:
            initial_data = memory_store.get_price_data(timeframe, market, symbol)
            if initial_data:
                await websocket.send_json(initial_data)

        # Kafka 구독 및 실시간 데이터 처리
        consumer = _create_consumer()
        while True:
            data = await consumer.get_message()
            if data['symbol'] == symbol and data['timeframe'] == timeframe:
                memory_store.update_realtime_data(data)  # 메모리 저장소 업데이트
                await websocket.send_json(data)

    except WebSocketDisconnect:
        print(f"WebSocket disconnected: {symbol}")
    except Exception as e:
        print(f"WebSocket error: {e}")

# @app.websocket("/ws/statistics/{data_type}/{timeframe}")
# async def statistics_websocket_endpoint(websocket: WebSocket, data_type: str, timeframe: str, market: str):
#     """통계 데이터 웹소켓 엔드포인트"""
#     await websocket.accept()
#     try:
#         # 초기 데이터 전송 (메모리 저장소에서)
#         if memory_store.loading_status[timeframe]:
#             initial_data = memory_store.get_statistics_data(timeframe, market)
#             if initial_data:
#                 await websocket.send_json(initial_data)

#         # Kafka 구독 및 실시간 데이터 처리
#         consumer = _create_consumer()
#         while True:
#             msg = consumer.poll(1.0)
#             if msg is None:
#                 break
#             if msg.error():
#                 continue
            
#             data = json.loads(msg.value().decode('utf-8'))
#             if data['type'] == data_type and data['timeframe'] == timeframe and data['market'] == market:
#                 memory_store.update_realtime_data(data)  # 메모리 저장소 업데이트
#                 await websocket.send_json(data)

#     except WebSocketDisconnect:
#         print(f"WebSocket disconnected: {market}")
#     except Exception as e:
#         print(f"WebSocket error: {e}")
@app.websocket("/ws/statistics/{data_type}")
async def statistics_websocket_endpoint(websocket: WebSocket, data_type: str):
    """통계 데이터 웹소켓 엔드포인트"""
    await websocket.accept()
    try:
        # Kafka 구독 및 실시간 데이터 처리
        consumer = create_kafka_consumer(Config.KAFKA_TOPICS['STATISTICS_DATA'], from_beginning = False)
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                await asyncio.sleep(0.1)
                continue
            
            data = json.loads(msg.value().decode('utf-8'))
            memory_store.update_realtime_data(data)  # 메모리 저장소 업데이트
            if data['type'] == data_type:
                await websocket.send_json(data)

    except WebSocketDisconnect:
        print(f"WebSocket disconnected")
    except Exception as e:
        print(f"WebSocket error: {e}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)