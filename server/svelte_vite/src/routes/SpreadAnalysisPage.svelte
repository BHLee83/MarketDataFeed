<script>
  import { onMount, onDestroy } from 'svelte';
  import { memoryStoreStatus } from '../lib/store.js';
  import { dbStore, fetchStatisticsData, getLastDate } from '../lib/db.js';
  import { realtimeStore, connectStatisticsWebSocket } from '../lib/realtime.js';
  import SpreadChart from '../components/SpreadChart.svelte';
    import { nonpassive } from 'svelte/legacy';

  let markets = ['EQ', 'IR', 'FX', 'CM', 'ECO'];
  let isLoading = false;
  let error = null;

  const timeFrames = [
    { value: '1m', label: '1분' },
    { value: '3m', label: '3분' },
    { value: '5m', label: '5분' },
    { value: '10m', label: '10분' },
    { value: '15m', label: '15분' },
    { value: '30m', label: '30분' },
    { value: '1d', label: '1일' }
  ]

  const dateRanges = [
    { value: 'today', label: '오늘' },
    { value: '1d', label: '1일' },
    { value: '1w', label: '1주일' },
    { value: '1m', label: '1개월' },
    { value: '1y', label: '1년' },
    { value: 'All', label: '전체' }
  ];

  // 마켓별 상태 관리
  let marketStates = markets.reduce((acc, market) => {
    acc[market] = {
      selectedTimeframe: '1d',
      selectedDateRange: '1m',
      spreadCharts: [],  // 각 마켓별 차트 데이터 배열
      isLoading: false,
      collapsed: false  // 접기/펼치기 상태 추가
    };
    return acc;
  }, {});

  // 공통 웹소켓 연결
  let statisticsWs = null;

  // 날짜 범위 계산
  async function calculateDateRange(range) {
    const end = new Date();
    let start = new Date();
    
    if (range === '1d') {
      try {
        const lastDate = await getLastDate(range);
        if (lastDate) {
          start = new Date(lastDate.replace(/(\d{4})(\d{2})(\d{2})/, '$1-$2-$3'));
        }
      } catch (e) {
        console.error('최신 날짜 조회 실패:', e);
        start.setDate(start.getDate() - 3);  // fallback
      }
    } else {
      // 기존 로직 유지
      switch(range) {
        case 'today': start.setHours(0,0,0,0); break;
        case '1d': start.setDate(start.getDate() - 1); break;
        case '1w': start.setDate(start.getDate() - 7); break;
        case '1m': start.setMonth(start.getMonth() - 1); break;
      }
    }
    
    return {
      start: start.toISOString().split('T')[0].replace(/-/g, ''),
      end: end.toISOString().split('T')[0].replace(/-/g, '')
    };
  }

  // 마켓별 데이터 로드
  async function loadMarketData(market) {
    try {
      const state = marketStates[market];
      state.isLoading = true;
      
      const { start, end } = await calculateDateRange(state.selectedDateRange);
      const data = await fetchStatisticsData(
        'spread',
        state.selectedTimeframe, 
        market, 
        null, 
        start, 
        end
      );

      if (data) {
        state.spreadCharts = Object.entries(data).map(([key, values]) => {
          // 데이터 형식 검증 및 변환
          const processedValues = Array.isArray(values) ? values : [values];
          return {
            key,
            data: processedValues.map(item => ({
              ...item,
              trd_date: item.trd_date,
              trd_time: item.trd_time,
              value1: item.value1,
              value2: item.value2
            }))
          };
        });
      }

      marketStates[market] = { ...state, isLoading: false };
    } catch (err) {
      error = err.message;
      console.error(`[${market}] 데이터 로딩 에러:`, err);
    }
  }

  // 데이터 구조 통일
  function mergeSpreadData(oldData = {}, data) {
    console.log('mergeSpreadData 입력:', { oldData, data });
    
    Object.entries(data).forEach(([key, value]) => {
        if (!Array.isArray(value)) {
            oldData[key] = value;
            return;
        }
        
        const items = [...value];
        items.sort((a, b) => {
            const dateA = parseInt(a.trd_date);
            const dateB = parseInt(b.trd_date);
            if (dateA !== dateB) return dateA - dateB;
            return parseInt(a.trd_time || '0') - parseInt(b.trd_time || '0');
        });
        
        oldData[key] = items[items.length - 1];
    });

    return oldData;
  }

  // 공통 WebSocket 설정
  function setupWebSocket() {
    if (statisticsWs) {
      statisticsWs.close();
    }
    statisticsWs = connectStatisticsWebSocket('spread');
    
    // 실시간 데이터 스토어 구독
    const unsubscribe = realtimeStore.subscribe(storeState => {
      const key = `statistics-spread`;
      const realtimeData = storeState.statisticsData.get(key);
      if (realtimeData && realtimeData.length > 0) {
        const lastData = realtimeData[realtimeData.length - 1];
        updateMarketData(lastData.market, lastData);
      }
    });

    return unsubscribe;
  }

  // 실시간 데이터 업데이트
  function updateMarketData(market, data) {
    const state = marketStates[market];
    const key = `${data.symbol1}-${data.symbol2}`;
    
    const chartIndex = state.spreadCharts.findIndex(chart => chart.key === key);
    if (chartIndex >= 0) {
      state.spreadCharts[chartIndex].data = [...state.spreadCharts[chartIndex].data, data];
      marketStates[market] = { ...state };
    }
  }

  // 스프레드 선택 처리 함수 추가
  async function handleSpreadSelect(market, spreadKey) {
    const state = marketStates[market];
    state.selectedSpread = spreadKey;
    
    // 선택된 스프레드의 차트 데이터 로드
    const { start, end } = await calculateDateRange(state.selectedDateRange);
    const data = await fetchStatisticsData('spread', state.selectedTimeframe, market, spreadKey, start, end);
    state.chartData = data[spreadKey] || [];
    console.log(`[${market}] 차트 데이터 업데이트:`, state.chartData);
    
    marketStates[market] = { ...state };
  }

  // 타임프레임 변경 처리 함수 수정
  async function handleTimeframeChange(market) {
    const state = marketStates[market];
    state.isLoading = true;  // 로딩 상태 추가
    
    try {
        const { start, end } = await calculateDateRange(state.selectedDateRange);
        const data = await fetchStatisticsData('spread', state.selectedTimeframe, market, null, start, end);
        
        // 기존 데이터 유지하면서 업데이트
        state.spreadData = mergeSpreadData(state.spreadData, data);
        
        if (state.selectedSpread) {
            state.chartData = data[state.selectedSpread] || [];
        }
    } catch (error) {
        console.error(`타임프레임 변경 중 오류: ${error}`);
    } finally {
        state.isLoading = false;
        marketStates[market] = { ...state };
    }
  }

  // 날짜 범위 변경 처리
  async function handleDateRangeChange(market) {
    const state = marketStates[market];
    if (state.selectedSpread) {
      // 선택된 스프레드의 차트 데이터만 업데이트
      const { start, end } = await calculateDateRange(state.selectedDateRange);
      const data = await fetchStatisticsData('spread', state.selectedTimeframe, market, state.selectedSpread, start, end);
      state.chartData = data[state.selectedSpread] || [];
      marketStates[market] = { ...state };
    }
  }

  // 접기/펼치기 토글 함수
  function toggleMarket(market) {
    marketStates[market].collapsed = !marketStates[market].collapsed;
    marketStates = marketStates;  // Svelte 반응성 트리거
  }

  onMount(() => {
    console.log('컴포넌트 마운트 시작');
    
    const unsubscribe = memoryStoreStatus.subscribe(status => {
      isLoading = status.isLoading;
    });

    (async () => {
      for (const market of markets) {
        console.log(`[${market}] 초기 데이터 로드 시작`);
        await loadMarketData(market);
      }
      console.log('웹소켓 연결 설정');
      setupWebSocket();
    })();

    return () => {
      console.log('컴포넌트 언마운트');
      if (statisticsWs) {
        statisticsWs.close();
      }
      unsubscribe();
    };
  });
</script>

<div class="markets-grid">
  {#each markets as market}
    <div class="market-section">
      <div class="market-header">
        <div class="header-left">
          <button class="toggle-btn" on:click={() => toggleMarket(market)}>
            {marketStates[market].collapsed ? '▶' : '▼'}
          </button>
          <h3>{market}</h3>
        </div>
        <div class="controls">
          <select 
            bind:value={marketStates[market].selectedTimeframe}
            disabled={marketStates[market].isLoading}
            on:change={() => loadMarketData(market)}
          >
            {#each timeFrames as frame}
              <option value={frame.value}>{frame.label}</option>
            {/each}
          </select>
          <select
            bind:value={marketStates[market].selectedDateRange}
            disabled={marketStates[market].isLoading}
            on:change={() => loadMarketData(market)}
          >
            {#each dateRanges as range}
              <option value={range.value}>{range.label}</option>
            {/each}
          </select>
        </div>
      </div>

      {#if !marketStates[market].collapsed}
        <div class="charts-container" class:collapsed={marketStates[market].collapsed}>
          {#if marketStates[market].isLoading}
            <div class="loading">데이터 로딩 중...</div>
          {:else}
            <div class="charts-grid">
              {#each marketStates[market].spreadCharts as chart}
                <div class="chart-item">
                  <div class="chart-title">{chart.key}</div>
                  <SpreadChart 
                    spreadData={chart.data}
                    timeframe={marketStates[market].selectedTimeframe}
                    dateRange={marketStates[market].selectedDateRange}
                  />
                </div>
              {/each}
            </div>
          {/if}
        </div>
      {/if}
    </div>
  {/each}
</div>

<style>
  .markets-grid {
    display: flex;
    flex-direction: column;
    gap: 20px;
    padding: 10px;
  }

  .market-section {
    border: 1px solid #ddd;
    border-radius: 8px;
    padding: 10px;
  }

  .market-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 10px;
  }

  .header-left {
    display: flex;
    align-items: center;
    gap: 8px;
  }

  .toggle-btn {
    background: none;
    border: none;
    cursor: pointer;
    padding: 4px;
    font-size: 12px;
    color: #666;
  }

  .toggle-btn:hover {
    color: #333;
  }

  .collapsed {
    display: none;
  }

  .charts-grid {
    display: grid;
    grid-template-columns: repeat(5, 1fr);  /* 4개의 열로 변경 */
    /* gap: 10px; */
    overflow-x: auto;
    /* padding: 10px; */
  }

  .chart-item {
    min-width: 250px;  /* 최소 너비 설정 */
    height: 150px;     /* 고정 높이 설정 */
    border: 1px solid #eee;
    border-radius: 4px;
    padding: 5px;
  }

  .chart-title {
    font-size: 12px;
    font-weight: bold;
    text-align: center;
    margin-bottom: 5px;
  }

  .loading {
    text-align: center;
    padding: 20px;
    color: #666;
  }

  select {
    padding: 4px 8px;
    border: 1px solid #ddd;
    border-radius: 4px;
  }

  h3 {
    margin: 0;
    font-size: 14px;
  }
</style> 