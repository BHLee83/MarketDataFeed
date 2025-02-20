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
  let isUpdated = false;
  let updateTimer;

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
    
    // 먼저 모든 데이터 포인트를 생성
    const processedData = rawData.map(item => {
        try {
            if (!item) return null;

            const dates = Array.isArray(item.trd_date) ? item.trd_date : [item.trd_date];
            const times = Array.isArray(item.trd_time) ? item.trd_time : [item.trd_time];
            const values = Array.isArray(item.value1) ? item.value1 : [item.value1];

            return dates.map((date, index) => {
                const trd_time = times[index] || 0;
                const value = values[index];
                const timeStr = String(trd_time).padStart(4, '0');
                const minutes = parseInt(timeStr.substring(0, 2)) * 60 + 
                              parseInt(timeStr.substring(2, 4));

                const dateObj = new Date(
                    parseInt(String(date).substring(0, 4)),
                    parseInt(String(date).substring(4, 6)) - 1,
                    parseInt(String(date).substring(6, 8)),
                    parseInt(timeStr.substring(0, 2)),
                    parseInt(timeStr.substring(2, 4))
                );

                if (isNaN(dateObj.getTime())) {
                    console.error('Invalid date:', {date, trd_time});
                    return null;
                }

                return {
                    date: dateObj,
                    value: value,
                    minutes: minutes
                };
            });
        } catch (error) {
            console.error('Data processing error:', error, item);
            return null;
        }
    })
    .filter(Boolean)
    .flat()
    .sort((a, b) => a.date - b.date);

    // 실시간 데이터(1분봉) 처리 및 통합
    if (rawData.length > 0 && rawData[rawData.length - 1].timeframe === '1m') {
        const tfMinutes = timeframe === '1d' ? 1440 : parseInt(timeframe.replace('m', ''));
        const groupedData = new Map();
        
        processedData.forEach(point => {
            if (timeframe === '1d') {
                const dateKey = point.date.toISOString().split('T')[0];
                const existingPoint = groupedData.get(dateKey);
                
                if (!existingPoint || point.date > existingPoint.date) {
                    groupedData.set(dateKey, {
                        ...point,
                        isUpdated: point.isUpdated
                    });
                }
            } else {
                // 현재 시간을 타임프레임의 끝으로 조정
                const targetMinutes = Math.ceil(point.minutes / tfMinutes) * tfMinutes;
                const dateKey = `${point.date.toISOString().split('T')[0]}-${targetMinutes}`;
                
                // 해당 타임프레임의 시작 시간 계산
                const frameStartMinutes = targetMinutes - tfMinutes;
                
                // 현재 데이터의 시간이 해당 타임프레임 구간에 속하는 경우에만 처리
                if (point.minutes > frameStartMinutes && point.minutes <= targetMinutes) {
                    const existingPoint = groupedData.get(dateKey);
                    if (!existingPoint || point.date > existingPoint.date) {
                        const adjustedDate = new Date(point.date);
                        adjustedDate.setMinutes(Math.floor(targetMinutes % 60));
                        adjustedDate.setHours(Math.floor(targetMinutes / 60));
                        
                        groupedData.set(dateKey, {
                            ...point,
                            date: adjustedDate,
                            minutes: targetMinutes,
                            isUpdated: point.isUpdated
                        });
                    }
                }
            }
        });

        return Array.from(groupedData.values())
            .sort((a, b) => a.date - b.date);
    }

    return processedData;
  }

  // 데이터나 타임프레임이 변경될 때마다 차트 갱신
  $: if (spreadData || timeframe || dateRange) {
    if (chartContainer) {
      updateChart();
      // // 데이터 업데이트 시 하이라이트 효과
      // isUpdated = true;
      // setTimeout(() => {
      //   isUpdated = false;
      // }, 1000);
    }
  }

  // 데이터 변경 감지
  $: {
    if (spreadData && spreadData.length > 0) {
      const lastData = spreadData[spreadData.length - 1];
      if (lastData.isUpdated) {
        // 이전 타이머 제거
        if (updateTimer) clearTimeout(updateTimer);
        
        isUpdated = true;
        updateTimer = setTimeout(() => {
          isUpdated = false;
          // 플래그 초기화
          lastData.isUpdated = false;
        }, 1000);
      }
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

<div bind:this={chartContainer} class="chart-container" class:updated={isUpdated}>
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
    transition: background-color 0.3s ease;
  }

  .updated {
    background-color: rgba(33, 150, 243, 0.1);
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