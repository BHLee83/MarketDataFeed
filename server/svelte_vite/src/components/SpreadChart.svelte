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

  const margin = { top: 20, right: 30, bottom: 30, left: 60 };  // 컴포넌트 레벨로 이동

  $: if (spreadData && chartContainer) {
    updateChart();
  }

  function updateChart() {
    if (!chartContainer || !spreadData.length) return;

    // 기존 차트와 이전 스케일 완전히 제거
    d3.select(chartContainer).selectAll("*").remove();

    // 차트 크기 설정
    const margin = { top: 20, right: 30, bottom: 30, left: 60 };
    width = chartContainer.clientWidth - margin.left - margin.right;
    height = chartContainer.clientHeight - margin.top - margin.bottom;

    // 데이터 처리
    const chartData = spreadData
      .map(d => {
        try {
          const dateStr = d.trd_date.toString().padStart(8, '0');
          const timeStr = d.trd_time.trim()? d.trd_time.toString().padStart(4, '0') : '0000';
          
          return {
            dateTime: `${dateStr}-${timeStr}`,
            date: new Date(
              dateStr.replace(/(\d{4})(\d{2})(\d{2})/, '$1-$2-$3') + 
              'T' + 
              timeStr.replace(/(\d{2})(\d{2})/, '$1:$2:00')
            ),
            value: parseFloat(d.value1),
            symbol1: d.symbol1,
            symbol2: d.symbol2
          };
        } catch (e) {
          console.error('Data processing error:', e, d);
          return null;
        }
      })
      .filter(d => d && !isNaN(d.value) && d.date instanceof Date && !isNaN(d.date))
      .sort((a, b) => a.date - b.date);

    if (!chartData.length) {
      if (spreadData && spreadData.length) {
        console.error('Data processing failed: No valid data after processing');
      }
      return;
    }

    // SVG 생성
    const svg = d3.select(chartContainer)
      .append("svg")
      .attr("width", width + margin.left + margin.right)
      .attr("height", height + margin.top + margin.bottom)
      .append("g")
      .attr("transform", `translate(${margin.left},${margin.top})`);

    // X축 스케일 (비연속 시계열)
    const xScale = d3.scalePoint()
      .domain(chartData.map(d => d.dateTime))
      .range([0, width])
      .padding(0.5);

    // Y축 스케일 - 매번 새로 계산
    const yScale = d3.scaleLinear()
      .domain([
        d3.min(chartData, d => d.value),
        d3.max(chartData, d => d.value)
      ])
      .range([height, 0]);

    // 축 설정
    const xAxis = d3.axisBottom(xScale)
      .tickFormat(d => {
        const [date, time] = d.split('-');
        return timeframe === '1d' 
            ? `${date.slice(0,4)}-${date.slice(4,6)}-${date.slice(6,8)}`
            : `${date.slice(4,6)}-${date.slice(6,8)} ${time.slice(0,2)}:${time.slice(2,4)}`;
      })
      .tickValues(xScale.domain().filter((_, i) => 
        i % Math.ceil(chartData.length / 10) === 0
      ));

    const yAxis = d3.axisLeft(yScale)
      .tickFormat(d => d.toFixed(2))
      .ticks(5);

    // 축 그리기
    svg.append("g")
      .attr("transform", `translate(0,${height})`)
      .call(xAxis)
      .selectAll("text")
      .style("text-anchor", "end")
      .attr("dx", "-.8em")
      .attr("dy", ".15em")
      .attr("transform", "rotate(-45)");

    svg.append("g")
      .call(yAxis);

    // 라인 생성
    const line = d3.line()
      .x(d => xScale(d.dateTime))
      .y(d => yScale(d.value));

    // 라인 그리기
    svg.append("path")
      .datum(chartData)
      .attr("fill", "none")
      .attr("stroke", "steelblue")
      .attr("stroke-width", 1.5)
      .attr("d", line);

    // 데이터 포인트 그리기
    const dots = svg.selectAll(".dot")
      .data(chartData)
      .enter()
      .append("circle")
      .attr("class", "dot")
      .attr("cx", d => xScale(d.dateTime))
      .attr("cy", d => yScale(d.value))
      .attr("r", 3)
      .attr("fill", d => d.value >= 0 ? "#2196F3" : "#f44336");

    // 툴팁 설정
    const tooltip = d3.select(chartContainer)
      .append("div")
      .attr("class", "tooltip")
      .style("opacity", 0);

    // 호버 효과
    dots.on("mouseover", (event, d) => {
      tooltip.transition()
        .duration(200)
        .style("opacity", .9);
      tooltip.html(
        `시간: ${d.date.toLocaleString()}<br/>
         값: ${d.value.toFixed(2)}`
      )
      .style("left", (event.pageX + 10) + "px")
      .style("top", (event.pageY - 28) + "px");
    })
    .on("mouseout", () => {
      tooltip.transition()
        .duration(500)
        .style("opacity", 0);
    });

    // 그리드 라인 추가
    svg.append("g")
      .attr("class", "grid")
      .call(d3.axisLeft(yScale)
        .tickSize(-width)
        .tickFormat("")
      );
  }

  // 데이터나 타임프레임이 변경될 때마다 차트 갱신
  $: if (spreadData || timeframe || dateRange) {
    if (chartContainer) {
        updateChart();
    }
  }

  onMount(() => {
    resizeObserver = new ResizeObserver(() => {
      if (chartContainer) {
        width = chartContainer.clientWidth - margin.left - margin.right;
        height = chartContainer.clientHeight - margin.top - margin.bottom;
        updateChart();
      }
    });
    resizeObserver.observe(chartContainer);
  });

  onDestroy(() => {
    if (resizeObserver) {
      resizeObserver.disconnect();
    }
  });
</script>

<div class="chart-container" bind:this={chartContainer}>
  {#if !spreadData || spreadData.length === 0}
    <div class="no-data">데이터가 없습니다</div>
  {/if}
</div>

<style>
  .chart-container {
    width: 100%;
    height: 80%;
    position: relative;
    background: white;
    border: 1px solid #ddd;
    border-radius: 4px;
  }

  .no-data {
    display: flex;
    justify-content: center;
    align-items: center;
    height: 100%;
    color: #666;
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