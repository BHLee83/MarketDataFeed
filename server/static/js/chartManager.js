class ChartManager {
    constructor() {
        console.log('ChartManager initialized');
        
        this.symbolSelect = document.getElementById('symbolSelect');
        this.timeframeSelect = document.getElementById('timeframeSelect');
        this.dateRangeSelect = document.getElementById('dateRangeSelect');
        
        if (!this.symbolSelect || !this.timeframeSelect || !this.dateRangeSelect) {
            console.error('Required DOM elements not found');
            return;
        }

        this.candleData = [];
        this.allData = [];
        this.maxCandles = this.getMaxCandles(this.dateRangeSelect.value);
        
        this.loadSymbols().then(() => {
            console.log('Symbols loaded');
            this.setupEventListeners();
            this.connectWebSocket();
        }).catch(error => {
            console.error('Error loading symbols:', error);
        });
    }

    async loadSymbols() {
        try {
            console.log('Fetching symbols...');
            const response = await fetch('/api/symbols');
            const data = await response.json();
            
            console.log('Received symbols:', data);
            
            this.symbolSelect.innerHTML = data.symbols
                .map(symbol => `<option value="${symbol}">${symbol}</option>`)
                .join('');
            
            // 첫 번째 종목 자동 선택
            // if (data.symbols.length > 0) {
            //     this.symbolSelect.value = data.symbols[0];
            // }
        } catch (error) {
            console.error('Error loading symbols:', error);
            throw error;
        }
    }

    setupEventListeners() {
        console.log('Setting up event listeners');
        
        this.symbolSelect.addEventListener('change', () => {
            const newSymbol = this.symbolSelect.value;
            console.log('Symbol changed:', newSymbol);
            
            this.clearChart();
            
            if (this.ws && this.ws.readyState === WebSocket.OPEN) {
                this.ws.send(JSON.stringify({
                    type: 'symbol_change',
                    symbol: newSymbol
                }));
            } else {
                this.connectWebSocket();
            }
        });
        
        this.timeframeSelect.addEventListener('change', () => {
            console.log('Timeframe changed:', this.timeframeSelect.value);
            this.clearChart();
            this.connectWebSocket();
        });
        
        this.dateRangeSelect.addEventListener('change', () => {
            console.log('Date range changed:', this.dateRangeSelect.value);
            this.maxCandles = this.getMaxCandles(this.dateRangeSelect.value);
            this.updateDisplayedData();
        });
    }

    getMaxCandles(dateRange) {
        // 타임프레임과 조회기간에 따른 최대 캔들 수 계산
        const timeframe = this.timeframeSelect.value;
        const candlesPerHour = {
            '1m': 60,
            '2m': 30,
            '3m': 20,
            '5m': 12,
            '10m': 6,
            '15m': 4,
            '30m': 2,
            '1h': 1,
            '1d': 1/24
        }[timeframe] || 60;

        switch(dateRange) {
            case 'today':
                const now = new Date();
                const hoursToday = now.getHours() + now.getMinutes()/60;
                return Math.ceil(hoursToday * candlesPerHour);
            case '1d':
                return candlesPerHour * 24;
            case '1w':
                return candlesPerHour * 24 * 7;
            case '1m':
                return candlesPerHour * 24 * 30;
            default:
                return 100;
        }
    }

    getTimeframeType(timeframe) {
        // 서버의 타임프레임 타입과 일치하도록 수정
        if (timeframe === '1d') return 'day';
        if (['1m', '2m', '3m', '5m', '10m', '15m', '30m'].includes(timeframe)) {
            return {
                timeframe: 'minute',
                interval: timeframe  // 실제 타임프레임 정보 추가
            };
        }
        return 'tick';
    }

    connectWebSocket() {
        const symbol = this.symbolSelect.value;
        const timeframeInfo = this.getTimeframeType(this.timeframeSelect.value);
        
        if (this.ws) {
            this.ws.onclose = null;
            this.ws.close();
            this.ws = null;
        }

        // URL 생성 시 타임프레임 정보 포함
        const wsUrl = `ws://${window.location.host}/ws/chart/${
            typeof timeframeInfo === 'object' ? timeframeInfo.timeframe : timeframeInfo
        }/${symbol}?interval=${
            typeof timeframeInfo === 'object' ? timeframeInfo.interval : ''
        }`;
        
        console.log('Connecting to WebSocket:', wsUrl);
        
        this.ws = new WebSocket(wsUrl);
        
        this.ws.onopen = () => {
            console.log('WebSocket connected');
            this.clearChart();
        };
        
        this.ws.onmessage = (event) => {
            console.log('Received WebSocket message:', event.data);
            try {
                const message = JSON.parse(event.data);
                if (message.type === 'historical_data') {
                    console.log('Received historical data:', message.data);
                    this.allData = message.data;
                    this.updateDisplayedData();
                }
                else if (message.type === 'market_data') {
                    console.log('Received market data:', message.data);
                    this.allData.push(message.data);
                    this.updateDisplayedData();
                }
            } catch (error) {
                console.error('Error processing message:', error);
                console.log('Raw message:', event.data);
            }
        };
        
        this.ws.onerror = (error) => {
            console.error('WebSocket error:', error);
        };
        
        this.ws.onclose = () => {
            console.log('WebSocket connection closed');
        };
    }

    updateDisplayedData() {
        if (this.allData.length === 0) return;

        const now = new Date();
        let startTime;

        switch(this.dateRangeSelect.value) {
            case 'today':
                startTime = new Date(now.setHours(0, 0, 0, 0));
                break;
            case '1d':
                startTime = new Date(now.getTime() - 24 * 60 * 60 * 1000);
                break;
            case '1w':
                startTime = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
                break;
            case '1m':
                startTime = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);
                break;
            default:
                startTime = new Date(0);
        }

        const processedData = this.processMarketData(this.allData);
        this.candleData = processedData.filter(d => d && d.date && d.date >= startTime);
        
        while (this.candleData.length > this.maxCandles) {
            this.candleData.shift();
        }

        this.renderChart(this.candleData);
    }

    clearChart() {
        this.candleData = [];
        this.allData = [];
        d3.select("#candleChart").selectAll("*").remove();
    }

    processMarketData(rawData) {
        // 데이터 형식 변환 및 거래시간 필터링
        const processedData = rawData.map(item => {
            const marketData = item.data;
            let timestamp;
            
            try {
                // 날짜 형식에서 구분자 제거
                const cleanDate = marketData.trd_date.replace(/-/g, '');
                
                // 날짜 형식 검증
                if (cleanDate.length === 8 && /^\d{8}$/.test(cleanDate)) {  // 유효한 8자리 숫자인지 확인
                    const year = parseInt(cleanDate.substring(0, 4));
                    const month = parseInt(cleanDate.substring(4, 6));
                    const day = parseInt(cleanDate.substring(6, 8));
                    
                    // ISO 형식의 날짜 문자열 생성
                    const dateStr = `${year}-${month.toString().padStart(2, '0')}-${day.toString().padStart(2, '0')}`;
                    const dateObj = new Date(dateStr);
                    
                    // 유효한 날짜인지 확인
                    if (isNaN(dateObj.getTime())) {
                        console.error('Invalid date:', marketData.trd_date);
                        return null;
                    }
                    
                    // 시간이 있는 경우 (분봉)
                    if (marketData.trd_time) {
                        const hour = marketData.trd_time.substring(0, 2);
                        const minute = marketData.trd_time.substring(2, 4);
                        timestamp = new Date(`${dateStr}T${hour}:${minute}:00`);
                    } else {  // 시간이 없는 경우 (일봉)
                        timestamp = dateObj;
                    }
                } else {
                    console.error('Unsupported date format:', marketData.trd_date);
                    return null;
                }

                return {
                    date: timestamp,
                    open: marketData.open,
                    high: marketData.high,
                    low: marketData.low,
                    close: marketData.close,
                    volume: marketData.volume
                };
            } catch (error) {
                console.error('Error processing market data:', error);
                return null;
            }
        }).filter(item => item !== null);  // 잘못된 데이터 필터링

        // 동일한 timestamp를 가진 데이터 중 마지막 데이터만 유지
        const uniqueData = {};
        processedData.forEach(item => {
            const timeKey = item.date.getTime();
            uniqueData[timeKey] = item;
        });

        // 객체를 다시 배열로 변환하고 시간순으로 정렬
        const finalData = Object.values(uniqueData).sort((a, b) => a.date - b.date);
        console.log('Processed market data:', finalData);

        return finalData;
    }

    renderChart(rawData) {
        d3.select("#candleChart").selectAll("*").remove();
        if (rawData.length === 0) return;

        const data = rawData;
        if (!data[0].date)
            data = this.processMarketData(rawData);

        // 차트 크기 설정
        const margin = {top: 20, right: 30, bottom: 30, left: 60};
        const width = 800 - margin.left - margin.right;
        const height = 400 - margin.top - margin.bottom;

        // SVG 생성
        const svg = d3.select("#candleChart")
            .append("svg")
            .attr("width", width + margin.left + margin.right)
            .attr("height", height + margin.top + margin.bottom)
            .append("g")
            .attr("transform", `translate(${margin.left},${margin.top})`);

        // 연속된 인덱스 생성
        const xIndices = data.map((_, index) => index);

        // X축 스케일 설정 - 실제 시간이 아닌 인덱스 기반
        const xScale = d3.scaleLinear()
            .domain([0, data.length - 1])
            .range([0, width]);

        // Y축 스케일 설정
        const yScale = d3.scaleLinear()
            .domain([
                d3.min(data, d => Math.min(d.low, d.close)) * 0.999,
                d3.max(data, d => Math.max(d.high, d.close)) * 1.001
            ])
            .range([height, 0]);

        // X축 그리기 - 실제 시간을 표시
        const xAxis = d3.axisBottom(xScale)
            .tickFormat(i => {
                const d = data[Math.floor(i)];
                if (d) {
                    return d.date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
                }
                return '';
            })
            .ticks(10);

        svg.append("g")
            .attr("transform", `translate(0,${height})`)
            .call(xAxis);

        // Y축 그리기
        svg.append("g")
            .call(d3.axisLeft(yScale));

        // 캔들스틱 그리기
        const candlesticks = svg.selectAll(".candlestick")
            .data(data)
            .enter()
            .append("g")
            .attr("class", "candlestick");

        // 캔들스틱 심지(wick) 그리기
        candlesticks.append("line")
            .attr("class", "wick")
            .attr("x1", (d, i) => xScale(i))
            .attr("x2", (d, i) => xScale(i))
            .attr("y1", d => yScale(d.high))
            .attr("y2", d => yScale(d.low))
            .attr("stroke", d => d.open > d.close ? "blue" : "red");

        // 캔들스틱 몸통(body) 그리기
        candlesticks.append("rect")
            .attr("class", "body")
            .attr("x", (d, i) => xScale(i) - 3)
            .attr("y", d => yScale(Math.max(d.open, d.close)))
            .attr("width", 6)
            .attr("height", d => Math.abs(yScale(d.open) - yScale(d.close)))
            .attr("fill", d => d.open > d.close ? "blue" : "red");

        // 툴팁 설정
        const tooltip = d3.select("#candleChart")
            .append("div")
            .attr("class", "tooltip")
            .style("opacity", 0)
            .style("position", "absolute");

        // 호버 영역 및 툴팁 설정
        candlesticks.append("rect")
            .attr("class", "hover-area")
            .attr("x", (d, i) => xScale(i) - 5)
            .attr("y", 0)
            .attr("width", 10)
            .attr("height", height)
            .attr("fill", "transparent")
            .on("mouseover", function(event, d) {
                const chartBounds = d3.select("#candleChart").node().getBoundingClientRect();

                tooltip.transition()
                    .duration(200)
                    .style("opacity", .9);
                tooltip.html(
                    `날짜: ${d.date.toLocaleDateString()}<br/>
                    시간: ${d.date.toLocaleTimeString()}<br/>
                    시가: ${d.open}<br/>
                    고가: ${d.high}<br/>
                    저가: ${d.low}<br/>
                    종가: ${d.close}<br/>
                    거래량: ${d.volume}`
                )
                .style("left", Math.min(event.pageX - chartBounds.left + 10, chartBounds.width - 150) + "px") // 툴팁이 오른쪽을 넘지 않도록 제한
                .style("top", (event.pageY - chartBounds.top - 28) + "px"); // 툴팁의 y축 위치 설정
            })
            .on("mouseout", function(d) {
                tooltip.transition()
                    .duration(500)
                    .style("opacity", 0);
            });
    }
}

console.log('Creating ChartManager instance');
const chartManager = new ChartManager();