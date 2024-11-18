``` mermaid
graph TB
    subgraph Clients
        C1[인포맥스 단말]
        C2[블룸버그 단말]
    end

    subgraph Main Server
        MS[데이터 수집 서버]
        SH[Socket Handler]
        DP[Data Processor]
    end

    subgraph Processing Server
        PS[데이터 처리 서버]
        CA[데이터 가공]
        ST[통계 처리]
    end

    subgraph Web Server
        WS[FastAPI]
        WS1[REST API]
        WS2[WebSocket]
    end

    subgraph Message Queue
        MQ[Apache Kafka]
        MQ1[raw_market_data]
        MQ2[processed-data]
        MQ3[chart-data]
        MQ4[statistics-data]
        ZK[Zookeeper]
    end

    subgraph MariaDB
        DB[(Main DB)]
        MT[market_data]
        MM[market_data_minute]
        MT2[market_data_tick]
        MR[market_data_rt]
        CR[cache_realtime_data]
        CC[cache_chart_data]
        CS[cache_stats_data]
    end

    subgraph Monitoring
        PR[Prometheus]
        GF[Grafana]
        ELK[ELK Stack]
    end

    C1 --> SH
    C2 --> SH
    SH --> DP
    DP --> MS
    MS --> MQ1
    MQ1 --> PS
    PS --> CA
    PS --> ST
    CA --> MQ2
    ST --> MQ3
    ST --> MQ4
    MQ2 --> WS
    MQ3 --> WS
    MQ4 --> WS
    WS --> WS1
    WS --> WS2
    MS --> DB
    PS --> DB
    WS --> DB
    ZK --- MQ
    PR --> GF
    PR -.-> MS
    PR -.-> PS
    PR -.-> WS
    PR -.-> MQ
    PR -.-> DB
```
