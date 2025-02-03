-- 기존 테이블 삭제 (필요한 경우)
DROP TABLE statistics_price_1d;
DROP TABLE statistics_price_1m;
DROP TABLE statistics_price_3m;
DROP TABLE statistics_price_5m;
DROP TABLE statistics_price_10m;
DROP TABLE statistics_price_15m;
DROP TABLE statistics_price_30m;

-- 1분봉, 3분봉, 5분봉, 10분봉, 15분봉, 30분봉 테이블
CREATE TABLE statistics_price_1m (
    id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    trd_date VARCHAR2(8) NOT NULL,  -- 일자 (YYYYMMDD 형식)
    trd_time VARCHAR2(6) NOT NULL,  -- 시간 (HHMM or HHMMSS 형식)
    market VARCHAR2(10) NOT NULL,   -- 시장 구분
    symbol1 VARCHAR2(20) NOT NULL,   -- 종목 코드1
    symbol2 VARCHAR2(20) NOT NULL,   -- 종목 코드2
    type VARCHAR2(20) NOT NULL,      -- 계산 종류
    value1 NUMBER(15,6),
    value2 NUMBER(15,6),
    value3 NUMBER(15,6),
    created_at TIMESTAMP DEFAULT SYSTIMESTAMP,
    CONSTRAINT idx_1m_symbol1_2_datetime UNIQUE (symbol1, symbol2, trd_date, trd_time, type)
);

-- 일봉 테이블
CREATE TABLE statistics_price_1d (
    id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    trd_date VARCHAR2(8) NOT NULL,  -- 일자 (YYYYMMDD 형식)
    trd_time VARCHAR2(6) NOT NULL,  -- 시간 (000000 or ' ')
    market VARCHAR2(10) NOT NULL,   -- 시장 구분
    symbol1 VARCHAR2(20) NOT NULL,   -- 종목 코드1
    symbol2 VARCHAR2(20) NOT NULL,   -- 종목 코드2
    type VARCHAR2(20) NOT NULL,      -- 계산 종류
    value1 NUMBER(15,6),
    value2 NUMBER(15,6),
    value3 NUMBER(15,6),
    created_at TIMESTAMP DEFAULT SYSTIMESTAMP,
    CONSTRAINT idx_day_symbol1_2_date UNIQUE (symbol1, symbol2, trd_date, type)
);