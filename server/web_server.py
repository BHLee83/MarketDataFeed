import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Config
from fastapi import FastAPI, HTTPException, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, FileResponse
from confluent_kafka import Consumer
import json
import asyncio
from typing import Dict, List
import threading
from queue import Queue
from prometheus_fastapi_instrumentator import Instrumentator
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from database.db_manager import DatabaseManager
from datetime import datetime

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
    return FileResponse("static/index.html")

@app.get("/api/symbols")
async def get_symbols():
        db_manager = DatabaseManager()
        meta = db_manager.load_marketdata_meta()
        symbols = [i['symbol'] for i in meta]
        return {"symbols": symbols}

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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
