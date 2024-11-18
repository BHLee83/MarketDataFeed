@echo off
title Kafka Environment

REM 이전 프로세스 종료
taskkill /F /IM java.exe

REM 데이터 디렉토리 초기화
rd /s /q D:\kafka\zookeeper
rd /s /q D:\kafka\kafka-logs
mkdir D:\kafka\zookeeper
mkdir D:\kafka\kafka-logs

REM Zookeeper 실행
start "Zookeeper" cmd /k "bin\windows\zookeeper-server-start.bat config\zookeeper.properties"

REM 10초 대기
timeout /t 10 /nobreak

REM Kafka 브로커 실행
start "Kafka" cmd /k "bin\windows\kafka-server-start.bat config\custom-server.properties"

REM 5초 대기
timeout /t 5 /nobreak

REM 토픽 생성
bin\windows\kafka-topics.bat --create --topic raw_market_data --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1