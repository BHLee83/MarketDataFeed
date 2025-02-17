<script>
  import { onMount, onDestroy } from 'svelte';
  import * as d3 from 'd3';

  export let spreadData;
  export let timeframe;
  export let dateRange;

  let chartContainer;
  let width = 0;
  let height = 0;
  let resizeObserver;

  const margin = { top: 20, right: 50, bottom: 40, left: 60 };

  function getTimeFormat() {
    return timeframe === '1d' ? d3.timeFormat("%y/%m/%d") : d3.timeFormat("%m/%d %H:%M");
  }

  $: if (spreadData && chartContainer) {
    updateChart();
  }

  function updateChart() {
    if (!chartContainer || !spreadData.length) return;

    const containerWidth = chartContainer.offsetWidth;
    const containerHeight = chartContainer.offsetHeight;
    
    width = containerWidth - margin.left - margin.right;
    height = containerHeight - margin.top - margin.bottom;

    if (width <= 0 || height <= 0) return;

    d3.select(chartContainer).selectAll("*").remove();

    const svg = d3.select(chartContainer)
      .append("svg")
      .attr("width", containerWidth)
      .attr("height", containerHeight)
      .append("g")
      .attr("transform", `translate(${margin.left},${margin.top})`);

    const chartData = processData(spreadData);
    
    const x = d3.scaleTime()
      .domain(d3.extent(chartData, d => d.date))
      .range([0, width]);

    const y = d3.scaleLinear()
      .domain([
        d3.min(chartData, d => d.value),
        d3.max(chartData, d => d.value)
      ])
      .range([height, 0])
      .nice();  // 깔끔한 눈금을 위해 추가

    // x축 추가
    svg.append("g")
      .attr("transform", `translate(0,${height})`)
      .call(d3.axisBottom(x)
        .ticks(5)
        .tickFormat(getTimeFormat()))
      .selectAll("text")  // x축 레이블 회전
      .style("text-anchor", "end")
      .attr("dx", "-.8em")
      .attr("dy", ".15em")
      .attr("transform", "rotate(-25)");

    // y축 추가
    svg.append("g")
      .call(d3.axisLeft(y)
        .ticks(5)
        .tickSize(-width) // 그리드 라인 추가
        .tickSizeOuter(0)
        .tickPadding(5)
        .tickFormat(d => {
          if (Math.abs(d) >= 1000000) {
            return (d / 1000000).toFixed(1) + 'M';
          } else if (Math.abs(d) >= 1000) {
            return (d / 1000).toFixed(1) + 'K';
          }
          return d.toFixed(2);
        }))
        .selectAll(".tick line")
        .attr("stroke", "lightgray");
        // .attr("stroke-opacity", 0.5);


    // 라인 생성 및 그리기
    const line = d3.line()
      .x(d => x(d.date))
      .y(d => y(d.value))
      .defined(d => !isNaN(d.value));

    svg.append("path")
      .datum(chartData)
      .attr("class", "line")
      .attr("fill", "none")
      .attr("stroke", "#2196F3")
      .attr("stroke-width", 1.5)
      .attr("d", line);

    // 최대/최소값 점 표시
    const maxPoint = chartData.slice(0, -1).reduce((max, p) => p.value > max.value ? p : max);
    const minPoint = chartData.slice(0, -1).reduce((min, p) => p.value < min.value ? p : min);

    [maxPoint, minPoint].forEach(point => {
      svg.append("circle")
        .attr("cx", x(point.date))
        .attr("cy", y(point.value))
        .attr("r", 4)
        .style("fill", point === maxPoint ? "#4CAF50" : "#f44336");
    });

    // 현재값 표시
    const lastPoint = chartData[chartData.length - 1];
    svg.append("text")
      .attr("x", width + 5)
      .attr("y", y(lastPoint.value))
      .attr("dy", "0.35em")
      .style("font-size", "10px")
      .text(lastPoint.value.toFixed(2));

    // 툴팁 설정
    const tooltipDiv = d3.select(chartContainer)
      .append("div")
      .attr("class", "tooltip")
      .style("opacity", 0)
      .style("position", "absolute") 
      .style("background", "rgba(255, 255, 255, 0.9)")
      .style("padding", "8px")
      .style("border", "1px solid #ddd")
      .style("border-radius", "4px")
      .style("pointer-events", "none")
      .style("font-size", "10px")
      .style("box-shadow", "0 2px 4px rgba(0,0,0,0.1)");

    // 인터랙티브 영역
    svg.append("rect")
      .attr("class", "overlay")
      .attr("width", width+1)
      .attr("height", height)
      .style("opacity", 0)
      .on("mousemove", function(event) {
        const [xPos] = d3.pointer(event, this);
        const bisect = d3.bisector(d => d.date).left;
        const x0 = x.invert(xPos);
        const i = bisect(chartData, x0, 1);
        const d0 = chartData[i - 1];
        const d1 = chartData[i];
        const d = x0 - d0.date > d1.date - x0 ? d1 : d0;

        tooltipDiv.style("opacity", 1)
          .html(`${timeframe === '1d' ? 
            d.date.toLocaleDateString() : 
            `${d.date.toLocaleDateString()} ${d.date.toLocaleTimeString()}`
          }<br/>값: ${d.value.toFixed(2)}`)
          .style("left", `${event.pageX + 0}px`)
          .style("top", `${event.pageY - 0}px`);
      })
      .on("mouseout", () => tooltipDiv.style("opacity", 0));
  }

  function processData(rawData) {
    return rawData.map(item => {
      try {
        const cleanDate = item.trd_date.toString().padStart(8, '0');
        const timeStr = item.trd_time ? item.trd_time.toString().padStart(4, '0') : '0000';
        
        const year = parseInt(cleanDate.substring(0, 4));
        const month = parseInt(cleanDate.substring(4, 6)) - 1;
        const day = parseInt(cleanDate.substring(6, 8));
        const hour = parseInt(timeStr.substring(0, 2));
        const minute = parseInt(timeStr.substring(2, 4));
        
        const date = new Date(year, month, day, hour, minute);
        
        return {
          date,
          dateTime: `${cleanDate}-${timeStr}`,
          value: parseFloat(item.value1),
          symbol1: item.symbol1,
          symbol2: item.symbol2
        };
      } catch (error) {
        console.error('Data processing error:', error, item);
        return null;
      }
    })
    .filter(item => item !== null && !isNaN(item.value))
    .sort((a, b) => a.date - b.date);
  }

  // 데이터나 타임프레임이 변경될 때마다 차트 갱신
  $: if (spreadData || timeframe || dateRange) {
    if (chartContainer) {
        updateChart();
    }
  }

  onMount(() => {
    if (chartContainer) {
      resizeObserver = new ResizeObserver(() => {
        if (spreadData) updateChart();
      });
      resizeObserver.observe(chartContainer);
    }
    return () => {
      if (resizeObserver) resizeObserver.disconnect();
    };
  });
</script>

<div bind:this={chartContainer} class="chart-container">
  {#if !spreadData || spreadData.length === 0}
    <div class="no-data">데이터 없음</div>
  {/if}
</div>

<style>
  .chart-container {
    width: 100%;
    height: 100%;
    position: relative;
    min-height: 150px;
  }

  .tooltip {
    position: absolute;
    background: rgba(255, 255, 255, 0.9);
    border: 1px solid #ddd;
    border-radius: 4px;
    padding: 8px;
    font-size: 12px;
    pointer-events: none;
    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
  }

  .no-data {
    display: flex;
    justify-content: center;
    align-items: center;
    height: 100%;
    color: #666;
    font-size: 12px;
  }

  :global(.grid line) {
    stroke: #e0e0e0;
    stroke-opacity: 0.7;
    shape-rendering: crispEdges;
  }

  :global(.grid path) {
    stroke-width: 0;
  }

  :global(.dot) {
    transition: r 0.2s;
  }

  :global(.dot:hover) {
    r: 5;
  }
</style> 