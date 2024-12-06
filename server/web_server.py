import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Config
from fastapi import FastAPI, HTTPException, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, FileResponse
from confluent_kafka import Consumer
import json
import asyncio
from typing import List
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

# Kafka 메시지를 저장할 큐
message_queue = Queue()

# Kafka Consumer 설정
conf = {
    'bootstrap.servers': Config.KAFKA_BOOTSTRAP_SERVERS,  # Kafka 서버 주소
    'group.id': 'web_server_group',
    'auto.offset.reset': 'earliest' # earliest or latest 체크 필요
}

# WebSocket 연결을 관리하는 클래스
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

manager = ConnectionManager()

# Kafka 메시지를 백그라운드에서 처리하는 함수
def kafka_consumer_thread():
    consumer = Consumer(conf)
    consumer.subscribe([Config.KAFKA_TOPICS['PROCESSED_DATA']])
    
    while True:
        try:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                continue
            
            # 메시지를 큐에 저장
            data = json.loads(msg.value().decode('utf-8'))
            message_queue.put(data)
        except Exception as e:
            print(f"Kafka consumer error: {e}")

# Kafka consumer 스레드 시작
consumer_thread = threading.Thread(target=kafka_consumer_thread, daemon=True)
consumer_thread.start()

# Prometheus 메트릭스 수집기 설정
instrumentator = Instrumentator()

# 정적 파일 서비스 설정
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def read_root():
    return FileResponse("static/index.html")

@app.get("/api/processed-data/{data_type}")
async def get_processed_data(data_type: str):
    """Kafka의 PROCESSED_DATA 토픽에서 특정 타입의 데이터 조회"""
    try:
        messages = []
        # 큐에서 최대 100개의 메시지를 가져옴
        for _ in range(100):
            try:
                data = message_queue.get_nowait()
                if data.get('type') == data_type:
                    messages.append(data)
            except:
                break
                
        return JSONResponse(content=messages)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"데이터 조회 실패: {str(e)}")

@app.websocket("/ws/market-data")
async def websocket_endpoint(websocket: WebSocket):
    """실시간 시장 데이터 웹소켓 엔드포인트"""
    await manager.connect(websocket)
    try:
        while True:
            # 큐에서 메시지를 비동기적으로 가져옴
            try:
                data = message_queue.get_nowait()
                await websocket.send_json(data)
            except:
                # 큐가 비어있으면 잠시 대기
                await asyncio.sleep(0.1)
                
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        print(f"클라이언트 연결 종료")
    except Exception as e:
        print(f"WebSocket 오류: {e}")
        manager.disconnect(websocket)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
