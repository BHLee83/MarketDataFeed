import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Config
from fastapi import FastAPI, HTTPException, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, FileResponse
from confluent_kafka import Consumer
import json
from contextlib import asynccontextmanager
from prometheus_fastapi_instrumentator import Instrumentator
from fastapi.staticfiles import StaticFiles

app = FastAPI()

# Kafka Consumer 설정
conf = {
    'bootstrap.servers': Config.KAFKA_BOOTSTRAP_SERVERS,  # Kafka 서버 주소
    'group.id': 'web_server_group',
    'auto.offset.reset': 'earliest' # earliest or latest 체크 필요
}
consumer = Consumer(conf)
consumer.subscribe([Config.KAFKA_TOPICS['PROCESSED_DATA']])

# Prometheus 메트릭스 수집기 설정
instrumentator = Instrumentator()

app.mount("/static", StaticFiles(directory="static"), name="static")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """서버 시작 및 종료 시 호출되는 이벤트"""
    instrumentator.instrument(app).expose(app)  # 메트릭스 엔드포인트 설정
    yield
    consumer.close()  # 서버 종료 시 consumer 닫기

app.router.lifespan = lifespan

@app.get("/api/processed-data/{data_type}")
async def get_processed_data(data_type: str):
    """Kafka의 PROCESSED_DATA 토픽에서 특정 타입의 데이터 조회"""
    try:
        messages = []
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                break
            if msg.error():
                continue
            
            data = json.loads(msg.value().decode('utf-8'))
            # 요청된 데이터 타입에 맞는 데이터만 필터링
            if data.get('type') == data_type:
                messages.append(data)
                
        return JSONResponse(content=messages)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"데이터 조회 실패: {str(e)}")

@app.get("/")
async def read_root():
    return FileResponse("static/index.html")

@app.websocket("/ws/market-data")
async def websocket_endpoint(websocket: WebSocket):
    """실시간 시장 데이터 웹소켓 엔드포인트"""
    await websocket.accept()
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                continue
            
            data = json.loads(msg.value().decode('utf-8'))
            await websocket.send_json(data)
    except WebSocketDisconnect:
        print("클라이언트 연결 종료")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080, lifespan="on")
