<script>
  import { onMount } from 'svelte';
  import * as d3 from 'd3';

  export let symbol;
  export let timeframe;
  export let dateRange;
  export let chartType = 'candle';  // 'candle' 또는 'line'
  export let showControls = true;

  const dateRanges = [
    { value: 'today', label: '오늘' },
    { value: '1d', label: '1일' },
    { value: '1w', label: '1주일' },
    { value: '1m', label: '1개월' }
  ];

  let chartContainer;
  let ws = null;
  let allData = [];  // 전체 데이터 저장
  let chartData = [];  // 화면에 표시할 데이터
  
  function getTimeframeType(timeframe) {
    if (timeframe === '1d') return 'day';
    if (['1m', '3m', '5m', '10m', '15m', '30m'].includes(timeframe)) {
      return {
        timeframe: 'minute',
        interval: timeframe
      };
    }
    return 'tick';
  }

  function processMarketData(rawData) {
    const processedData = rawData.map(item => {
      const marketData = item.data;
      let timestamp;
      
      try {
        const cleanDate = marketData.trd_date.replace(/-/g, '');
        
        if (cleanDate.length === 8 && /^\d{8}$/.test(cleanDate)) {
          const year = parseInt(cleanDate.substring(0, 4));
          const month = parseInt(cleanDate.substring(4, 6));
          const day = parseInt(cleanDate.substring(6, 8));
          
          const dateStr = `${year}-${month.toString().padStart(2, '0')}-${day.toString().padStart(2, '0')}`;
          const dateObj = new Date(dateStr);
          
          if (isNaN(dateObj.getTime())) {
            console.error('Invalid date:', marketData.trd_date);
            return null;
          }
          
          if (marketData.trd_time) {
            const hour = marketData.trd_time.substring(0, 2);
            const minute = marketData.trd_time.substring(2, 4);
            timestamp = new Date(`${dateStr}T${hour}:${minute}:00`);
          } else {
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
    }).filter(item => item !== null);

    // 동일한 timestamp를 가진 데이터 중 마지막 데이터만 유지
    const uniqueData = {};
    processedData.forEach(item => {
      const timeKey = item.date.getTime();
      uniqueData[timeKey] = item;
    });

    return Object.values(uniqueData).sort((a, b) => a.date - b.date);
  }

  function updateDisplayedData() {
    if (allData.length === 0) return;

    const now = new Date();
    let startTime;

    switch(dateRange) {
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

    const processedData = processMarketData(allData);
    chartData = processedData.filter(d => d && d.date && d.date >= startTime);
    updateChart();
  }

  function updateChart() {
    if (!chartContainer || chartData.length === 0) return;
    
    // 기존 차트 제거
    d3.select(chartContainer).selectAll("*").remove();
    
    // 차트 크기 및 마진 설정
    const margin = { top: 20, right: 30, bottom: 30, left: 60 };
    const width = chartContainer.offsetWidth - margin.left - margin.right;
    const height = chartContainer.offsetHeight - margin.top - margin.bottom;
    
    if (width <= 0 || height <= 0) return;  // 크기가 유효하지 않으면 중단
    
    // SVG 생성
    const svg = d3.select(chartContainer)
      .append("svg")
      .attr("width", width + margin.left + margin.right)
      .attr("height", height + margin.top + margin.bottom)
      .append("g")
      .attr("transform", `translate(${margin.left},${margin.top})`);
    
    // 스케일 설정
    const xScale = d3.scaleLinear()
      .domain([0, chartData.length - 1])
      .range([0, width]);

    // Y축 스케일 설정
    const yScale = d3.scaleLinear()
      .domain([
        d3.min(chartData, d => d.low),
        d3.max(chartData, d => d.high)
      ])
      .range([height, 0]);
    
    // 축 설정
    const xAxis = d3.axisBottom(xScale)
      .tickFormat(i => {
        const d = chartData[i]?.date;
        return d ? d.toLocaleTimeString() : '';
      });
    
    const yAxis = d3.axisLeft(yScale);
    
    svg.append("g")
      .attr("transform", `translate(0,${height})`)
      .call(xAxis);

    // Y축 그리기
    svg.append("g")
      .call(yAxis);

    if (chartType === 'line') {
      // 라인 차트 그리기
      const line = d3.line()
        .x((d, i) => xScale(i))
        .y(d => yScale(d.close))
        .defined(d => !isNaN(d.close)); // null이나 NaN 값 처리

      // 라인 그리기
      svg.append("path")
        .datum(chartData)
        .attr("class", "line")
        .attr("fill", "none")
        .attr("stroke", "steelblue")
        .attr("stroke-width", 1.5)
        .attr("d", line);

      // // 데이터 포인트 그리기
      // svg.selectAll(".dot")
      //   .data(chartData)
      //   .enter()
      //   .append("circle")
      //   .attr("class", "dot")
      //   .attr("cx", (d, i) => xScale(i))
      //   .attr("cy", d => yScale(d.close))
      //   .attr("r", 3)
      //   .attr("fill", "steelblue");

      // 호버 이벤트를 위한 투명한 오버레이
      const overlay = svg.append("rect")
        .attr("class", "overlay")
        .attr("width", width)
        .attr("height", height)
        .style("fill", "none")
        .style("pointer-events", "all");

      // 툴팁 설정
      const tooltip = d3.select(chartContainer)
        .append("div")
        .attr("class", "tooltip")
        .style("opacity", 0);

      // 호버 이벤트 핸들러
      overlay.on("mousemove", function(event) {
        const x0 = xScale.invert(d3.pointer(event)[0]);
        const i = Math.round(x0);
        if (i >= 0 && i < chartData.length) {
          const d = chartData[i];
          const chartBounds = chartContainer.getBoundingClientRect();
          
          tooltip.transition()
            .duration(200)
            .style("opacity", .9);
          tooltip.html(
            `날짜: ${d.date.toLocaleDateString()}<br/>
            시간: ${d.date.toLocaleTimeString()}<br/>
            종가: ${d.close}<br/>
            거래량: ${d.volume}`
          )
          .style("left", `${event.clientX - chartBounds.left + 10}px`)
          .style("top", `${event.clientY - chartBounds.top - 28}px`);
        }
      })
      .on("mouseout", function() {
        tooltip.transition()
          .duration(500)
          .style("opacity", 0);
      });
    } else {
      // 캔들스틱 차트 그리기
      const candlesticks = svg.selectAll(".candlestick")
        .data(chartData)
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
    }
  }

  async function connectWebSocket() {
    if (ws) {
      ws.onclose = null;
      ws.close();
      ws = null;
    }

    const timeframeInfo = getTimeframeType(timeframe);
    const API_URL = import .meta.env.VITE_API_URL;
    const wsUrl = `${API_URL}/ws/chart/${
      typeof timeframeInfo === 'object' ? timeframeInfo.timeframe : timeframeInfo
    }/${symbol}?interval=${
      typeof timeframeInfo === 'object' ? timeframeInfo.interval : ''
    }`;

    console.log('Connecting to WebSocket:', wsUrl);
    
    ws = new WebSocket(wsUrl);
    
    ws.onopen = () => {
      console.log('WebSocket connected');
      allData = [];  // 새로운 연결시 데이터 초기화
    };

    ws.onmessage = (event) => {
      const data = JSON.parse(event.data);
      if (data.type === 'historical_data') {
        allData = data.data;
        updateDisplayedData();
      } else if (data.type === 'market_data') {
        allData = [...allData, data.data];
        updateDisplayedData();
      }
    };

    ws.onerror = (error) => {
      console.error('WebSocket 연결 오류:', error);
    };
  }

  $: if (symbol && timeframe) {
    connectWebSocket();
  }

  $: if (dateRange && allData.length > 0) {
    updateDisplayedData();
  }

</script>

{#if showControls}
  <div class="chart-controls">
    <select bind:value={dateRange}>
      {#each dateRanges as dr}
        <option value={dr.value}>{dr.label}</option>
      {/each}
    </select>
  </div>
{/if}

<div class="chart-container" bind:this={chartContainer}></div>

<style>
  .chart-controls {
    margin-bottom: 1rem;
  }

  .chart-controls select {
    padding: 5px 10px;
    font-size: 14px;
    border: 1px solid #ddd;
    border-radius: 4px;
    background-color: white;
  }

  .chart-container {
    width: 100%;
    height: 500px;
    background: white;
    border: 1px solid #ddd;
    border-radius: 4px;
    position: relative;
    overflow: hidden;
    margin: 0 auto;
  }

  :global(.tooltip) {
    position: absolute;
    padding: 8px;
    background: rgba(255, 255, 255, 0.9);
    border: 1px solid #ddd;
    border-radius: 4px;
    pointer-events: none;
    font-size: 12px;
    z-index: 100;
  }
</style> 