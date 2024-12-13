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
data_queues = {
    'tick': Queue(),
    'minute': Queue(),
    'daily': Queue()
}

# Kafka Consumer 설정
def create_consumer(topic: str):
    conf = {
        'bootstrap.servers': Config.KAFKA_BOOTSTRAP_SERVERS,
        'group.id': f'web_consumer_{topic}',
        'auto.offset.reset': 'latest'
    }
    consumer = Consumer(conf)
    consumer.subscribe([topic])
    return consumer

# Kafka Consumer 스레드
def kafka_consumer_thread(topic_type: str):
    topics = {
        'tick': Config.KAFKA_TOPICS['RAW_MARKET_DATA'],
        'minute': Config.KAFKA_TOPICS['RAW_MARKET_DATA_MINUTE'],
        'daily': Config.KAFKA_TOPICS['RAW_MARKET_DATA_DAY']
    }
    
    consumer = create_consumer(topics[topic_type])
    
    while True:
        try:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                continue
                
            data = json.loads(msg.value().decode('utf-8'))
            data_queues[topic_type].put(data)
        except Exception as e:
            print(f"Consumer error ({topic_type}): {e}")

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
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def read_root():
    return FileResponse("static/index.html")

@app.websocket("/ws/{data_type}")
async def websocket_endpoint(websocket: WebSocket, data_type: str):
    if data_type not in ['tick', 'minute', 'daily']:
        await websocket.close()
        return
        
    await manager.connect(websocket, data_type)
    
    try:
        while True:
            try:
                # 큐에서 데이터 가져오기
                data = data_queues[data_type].get_nowait()
                await websocket.send_json(data)
            except:
                await asyncio.sleep(0.1)
                
    except WebSocketDisconnect:
        manager.disconnect(websocket, data_type)
    except Exception as e:
        print(f"WebSocket error: {e}")
        manager.disconnect(websocket, data_type)

# Kafka consumer 스레드 시작
for data_type in ['tick', 'minute', 'daily']:
    thread = threading.Thread(
        target=kafka_consumer_thread,
        args=(data_type,),
        daemon=True
    )
    thread.start()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
