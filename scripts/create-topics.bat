@echo off
cd D:\kafka
bin\windows\kafka-topics.bat --create --topic raw_market_data --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
pause