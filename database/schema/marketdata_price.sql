-- 기존 테이블 삭제 (필요한 경우)
DROP TABLE marketdata_price_1d;

-- 1분봉, 3분봉, 5분봉, 10분봉, 15분봉, 30분봉 테이블
CREATE TABLE marketdata_price_30m (
    id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    symbol VARCHAR2(20) NOT NULL,   -- 종목 코드
    trd_date VARCHAR2(8) NOT NULL,  -- 일자 (YYYYMMDD 형식)
    trd_time VARCHAR2(6) NOT NULL,  -- 시간 (HHMM or HHMMSS 형식)
    open NUMBER(15,6),              -- 시가
    high NUMBER(15,6),              -- 고가
    low NUMBER(15,6),                 -- 저가
    close NUMBER(15,6),              -- 종가
    open_int NUMBER(20,6),          -- 미결제약정수량 (선물/옵션의 경우)
    theo_prc NUMBER(15,6),          -- 이론가 (선물/옵션의 경우)
    under_lvl NUMBER(15,6),         -- 기초자산가격
    trd_volume NUMBER(20,6),        -- 단위거래량
    trd_value NUMBER(25,6),         -- 단위거래대금
    created_at TIMESTAMP DEFAULT SYSTIMESTAMP,
    CONSTRAINT idx_30m_symbol_datetime UNIQUE (symbol, trd_date, trd_time)
);

-- 일봉 테이블
CREATE TABLE marketdata_price_1d (
    id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    symbol VARCHAR2(20) NOT NULL,   -- 종목 코드
    trd_date VARCHAR2(8) NOT NULL,  -- 일자 (YYYYMMDD 형식)
    trd_time VARCHAR2(6) NOT NULL,  -- 시간 (000000 or ' ')
    open NUMBER(15,6),              -- 시가
    high NUMBER(15,6),              -- 고가
    low NUMBER(15,6),                 -- 저가
    close NUMBER(15,6),              -- 종가
    open_int NUMBER(20,6),          -- 미결제약정수량 (선물/옵션의 경우)
    theo_prc NUMBER(15,6),          -- 이론가 (선물/옵션의 경우)
    under_lvl NUMBER(15,6),         -- 기초자산가격
    trd_volume NUMBER(20,6),         -- 단위거래량
    trd_value NUMBER(25,6),          -- 단위거래대금
    created_at TIMESTAMP DEFAULT SYSTIMESTAMP,
    CONSTRAINT idx_day_symbol_date UNIQUE (symbol, trd_date)
);