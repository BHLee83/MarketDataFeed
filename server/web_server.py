import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Config
from fastapi import FastAPI, HTTPException, Request, Response
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

@app.get("/api/processed-data")
async def get_processed_data():
    """Kafka의 PROCESSED_DATA 토픽에서 데이터 조회"""
    try:
        messages = []
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                break
            if msg.error():
                continue
            print(f"추출 내용: {msg.value().decode('utf-8')}")
            market_data = json.loads(msg.value().decode('utf-8'))
            messages.append(market_data)
        return JSONResponse(content=messages)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"데이터 조회 실패: {str(e)}")

@app.get("/")
async def read_root():
    return FileResponse("static/index.html")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=6000)
