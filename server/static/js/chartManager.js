class ChartManager {
    constructor() {
        this.minuteChartContainer = document.getElementById('minuteChart');
        this.dailyChartContainer = document.getElementById('dailyChart');
    }

    updateMinuteChart(data) {
        console.log('분봉 차트 데이터 수신:', data);
        // 분봉 차트 데이터를 차트 객체에 업데이트
        this.drawChart(this.minuteChartContainer, data, 'minute');
    }

    updateDailyChart(data) {
        // 일봉 차트 데이터를 차트 객체에 업데이트
        this.drawChart(this.dailyChartContainer, data, 'daily');
    }

    drawChart(container, data, type) {
        // 차트 컨테이너 내용을 초기화
        container.innerHTML = '';

        // SVG 요소 생성
        const svg = d3.select(container)
            .append('svg')
            .attr('width', 800)
            .attr('height', 400);

        // 데이터 매핑을 위한 스케일 설정
        const xScale = d3.scaleBand()
            .range([0, 780])
            .padding(0.1)
            .domain(data.map(d => d.timestamp));
        const yScale = d3.scaleLinear()
            .domain([0, d3.max(data, d => d.price)])
            .range([380, 20]);

        // 바 차트 그리기
        svg.selectAll(".bar")
            .data(data)
            .enter().append("rect")
            .attr("class", "bar")
            .attr("x", d => xScale(d.timestamp))
            .attr("y", d => yScale(d.price))
            .attr("width", xScale.bandwidth())
            .attr("height", d => 380 - yScale(d.price))
            .attr("fill", "steelblue");

        // X축 추가
        svg.append("g")
            .attr("transform", "translate(0,380)")
            .call(d3.axisBottom(xScale));

        // Y축 추가
        svg.append("g")
            .attr("transform", "translate(0,0)")
            .call(d3.axisLeft(yScale));
    }
}

// 차트 관리자 객체 생성 및 초기화
const chartManager = new ChartManager();