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

  const margin = { top: 20, right: 50, bottom: 60, left: 40 };

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
    
    const x = d3.scaleLinear()
      .domain([0, chartData.length - 1])
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
        .ticks(Math.min(
          chartData.length,
          Math.floor(width / 40)  // 레이블당 최소 40px 공간 확보
        ))
        .tickFormat(i => {
          if (i >= 0 && i < chartData.length) {
            return getTimeFormat()(chartData[Math.floor(i)].date);
          }
          return '';
        }))
      .selectAll("text")
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
      .x((d, i) => x(i))
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
    if (chartData.length > 0) {
        const maxPoint = chartData.slice(0, -1).reduce((max, p) => p.value > max.value ? p : max, chartData[0]);
        const minPoint = chartData.slice(0, -1).reduce((min, p) => p.value < min.value ? p : min, chartData[0]);

        [maxPoint, minPoint].forEach((point, i) => {
            const pointIndex = chartData.indexOf(point);  // 해당 포인트의 인덱스 찾기
            svg.append("circle")
                .attr("cx", x(pointIndex))  // 인덱스 기반 x 좌표
                .attr("cy", y(point.value))
                .attr("r", 3)
                .attr("fill", point === maxPoint ? "#f44336" : "#2196F3");

            svg.append("text")
                .attr("x", x(pointIndex))  // 인덱스 기반 x 좌표
                .attr("y", y(point.value) - 10)
                .attr("text-anchor", "middle")
                .attr("font-size", "10px")
                .text(point.value.toFixed(2));
        });
    }

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
      .attr("width", width)
      .attr("height", height)
      .attr("fill", "none")
      .attr("pointer-events", "all")
      .on("mousemove", function(event) {
        const [xPos, yPos] = d3.pointer(event, this);
        const i = Math.round(x.invert(xPos));  // 인덱스로 직접 변환
        
        if (i < 0 || i >= chartData.length) return;

        const d = chartData[i];  // 해당 인덱스의 데이터 직접 사용

        tooltipDiv
          .style("opacity", 1)
          .style("left", (event.pageX + 5) + "px")  // 마우스 x 포지션 사용  
          .style("top", (event.pageY + 5) + "px")  // 마우스 y 포지션 사용
          .html(`${getTimeFormat()(d.date)}<br>${d.value.toFixed(2)}`);
      })
      .on("mouseleave", function() {
        tooltipDiv.style("opacity", 0);
      });
  }

  function processData(rawData) {
    if (!Array.isArray(rawData)) {
        console.error('Invalid data format:', rawData);
        return [];
    }
    
    return rawData.map(item => {
        try {
            if (!item || !Array.isArray(item.trd_date)) {
                console.error('Invalid item format:', item);
                return null;
            }

            // 배열의 각 요소에 대해 날짜 객체 생성
            return item.trd_date.map((date, index) => {
                const trd_time = item.trd_time[index] || 0;
                const value = item.value1[index];

                // 날짜 문자열 생성
                const dateStr = String(date);
                const timeStr = String(trd_time).padStart(4, '0');
                
                const dateObj = new Date(
                    parseInt(dateStr.substring(0, 4)),
                    parseInt(dateStr.substring(4, 6)) - 1,
                    parseInt(dateStr.substring(6, 8)),
                    parseInt(timeStr.substring(0, 2)),
                    parseInt(timeStr.substring(2, 4))
                );

                if (isNaN(dateObj.getTime())) {
                    console.error('Invalid date:', {date, trd_time});
                    return null;
                }

                return {
                    date: dateObj,
                    value: value
                };
            });
        } catch (error) {
            console.error('Data processing error:', error, item);
            return null;
        }
    })
    .flat()
    .filter(item => item !== null)
    .sort((a, b) => a.date.getTime() - b.date.getTime());  // 날짜 오름차순 정렬
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
    /* position: relative; */
    min-height: 150px;
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