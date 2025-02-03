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

  const dateRanges = [
    { value: 'today', label: '오늘' },
    { value: '1d', label: '1일' },
    { value: '1w', label: '1주일' },
    { value: '1m', label: '1개월' }
  ];

  // 마켓별 상태 관리
  let marketStates = markets.reduce((acc, market) => {
    acc[market] = {
      selectedTimeframe: '1d',
      selectedDateRange: '1d',
      selectedSpread: null,
      spreadData: {},
      chartData: [],       // 차트를 위한 시계열 데이터
      isLoading: false,  // 마켓별 로딩 상태 추가
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
      console.log(`[${market}] 데이터 로드 시작`);
      const state = marketStates[market];
      const { start, end } = await calculateDateRange(state.selectedDateRange);
      
      console.log(`[${market}] 날짜 범위:`, { start, end });
      console.log(`[${market}] 타임프레임:`, state.selectedTimeframe);
      
      const data = await fetchStatisticsData('spread', state.selectedTimeframe, market, null, start, end);
      console.log(`[${market}] 받아온 데이터:`, data);
      
      if (state.selectedSpread) {
        console.log(`[${market}] 선택된 스프레드:`, state.selectedSpread);
        state.chartData = data[state.selectedSpread] || [];
        console.log(`[${market}] 차트 데이터:`, state.chartData);
      }
      
      state.spreadData = mergeSpreadData(state.spreadData, data);
      console.log(`[${market}] 병합된 스프레드 데이터:`, state.spreadData);
      
      marketStates[market] = { ...state };
      console.log(`[${market}] 최종 마켓 상태:`, marketStates[market]);

    } catch (err) {
      error = err.message;
      console.error(`[${market}] 데이터 로딩 에러:`, err);
    }
  }

  // 데이터 구조 통일
  function mergeSpreadData(oldData, data) {
    console.log('mergeSpreadData 입력:', { oldData, data });
    
    Object.keys(data).forEach(key => {
      const items = data[key];
      // 날짜, 시간 기준으로 정렬
      items.sort((a, b) => {
        const dateA = parseInt(a.trd_date);
        const dateB = parseInt(b.trd_date);
        if (dateA !== dateB) return dateA - dateB;
        return parseInt(a.trd_time || '0') - parseInt(b.trd_time || '0');
      });
      
      const lastItem = items[items.length - 1];
      oldData[key] = lastItem;
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
    console.log(`[${market}] 실시간 데이터 수신:`, data);
    
    const state = marketStates[market];
    const key = `${data.symbol1}-${data.symbol2}`;
    if (state.selectedSpread === key) {
      state.chartData = [...state.chartData, data];
      // console.log(`[${market}] 업데이트된 차트 데이터:`, state.chartData);
    }
    
    // state.spreadData = mergeSpreadData(state.spreadData, [data]);
    state.spreadData[key] = data;
    // console.log(`[${market}] 업데이트된 스프레드 데이터:`, state.spreadData[key]);
    
    // 1초 후 업데이트 표시 제거
    setTimeout(() => {
      if (state.spreadData[key]) {
        state.spreadData[key] = { ...state.spreadData[key], updated: false };
      }
      marketStates[market] = { ...state };
    }, 1000);

    marketStates[market] = { ...state };
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

<div class="markets">
  {#each markets as market}
    <div class="market-section">
      <div class="market-header">
        <h3>{market}</h3>
        <div class="controls">
          <select 
            bind:value={marketStates[market].selectedTimeframe}
            disabled={marketStates[market].isLoading}
            on:change={() => handleTimeframeChange(market)}
          >
            <option value="1m">1분</option>
            <option value="3m">3분</option>
            <option value="5m">5분</option>
            <option value="10m">10분</option>
            <option value="15m">15분</option>
            <option value="30m">30분</option>
            <option value="1d">1일</option>
          </select>
        </div>
      </div>

      <div class="spread-list">
        {#if marketStates[market].isLoading}
          <div class="loading">데이터 로딩 중...</div>
        {:else}
          {#each Object.entries(marketStates[market].spreadData) as [key, spread]}
            <button 
              class="spread-item" 
              class:selected={marketStates[market].selectedSpread === key}
              class:highlight={spread.updated}
              on:click={() => handleSpreadSelect(market, key)}
            >
              <div class="spread-info">
                <div class="spread-pair">{key}</div>
                <div class="spread-time">
                  {spread.trd_date.replace(/(\d{4})(\d{2})(\d{2})/, '$1-$2-$3')} 
                  {spread.trd_time?.replace(/(\d{2})(\d{2})/, '$1:$2:00')}
                </div>
              </div>
              <div class="spread-value" class:negative={spread.value1 < 0}>
                {spread.value1?.toFixed(2)}
              </div>
            </button>
          {/each}
        {/if}
      </div>

      <div class="chart-section">
        <div class="chart-header">
          <select 
            bind:value={marketStates[market].selectedDateRange}
            disabled={marketStates[market].isLoading}
            on:change={() => handleDateRangeChange(market)}
          >
            {#each dateRanges as range}
              <option value={range.value}>{range.label}</option>
            {/each}
          </select>
        </div>
        
        {#if marketStates[market].selectedSpread}
          <SpreadChart 
            
            spreadData={marketStates[market].chartData}
            timeframe={marketStates[market].selectedTimeframe}
            dateRange={marketStates[market].selectedDateRange}
          />
        {:else}
          <div class="no-chart">스프레드를 선택하세요</div>
        {/if}
      </div>
    </div>
  {/each}
</div>

<style>
  .markets {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 20px;
    padding: 20px;
  }
  
  .market-section {
    border: 1px solid #ddd;
    padding: 15px;
    border-radius: 10px;
    width: 100%;
    box-sizing: border-box;
  }

  .market-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 15px;
  }

  .spread-list {
    height: 400px;
    overflow-y: auto;
    border: 1px solid #eee;
    width: 100%;
  }

  .spread-item {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 12px;
    height: 60px;
    /* min-height: 40px; */
    border-bottom: 1px solid #eee;
    width: 95%;
    /* box-sizing: border-box; */
    /* background: none; */
    /* border: none; */
    /* text-align: left; */
    cursor: pointer;
    transition: all 0.3s ease;
  }

  .spread-item:hover {
    background-color: #f5f5f5;
  }

  .spread-item.selected {
    background-color: #e3f2fd;
  }

  .spread-info {
    display: flex;
    flex-direction: column;
    gap: 4px;
  }

  .spread-pair {
    font-weight: 500;
    /* color: #333; */
  }

  .spread-time {
    font-size: 0.9em;
    color: #666;
  }

  .spread-value {
    font-size: 1.1em;
    font-weight: 600;
    color: #2196F3;
  }

  .spread-value.negative {
    color: #f44336;
  }

  .chart-section {
    margin-top: 15px;
    height: 200px;
    width: 100%;
  }

  .chart-header {
    display: flex;
    justify-content: flex-end;
    gap: 10px;
    margin-bottom: 10px;
  }

  select {
    padding: 2px 4px;
    border: 1px solid #ddd;
    border-radius: 4px;
    /* appearance: none; */
    /* background-color: white; */
  }

  .no-data, .no-chart {
    display: flex;
    justify-content: center;
    align-items: center;
    height: 100%;
    color: #666;
  }

  /* 스크롤바 스타일링 */
  .spread-list::-webkit-scrollbar {
    width: 8px;
    height: 8px;
  }

  .spread-list::-webkit-scrollbar-track {
    background: #f1f1f1;
  }

  .spread-list::-webkit-scrollbar-thumb {
    background: #888;
    border-radius: 4px;
  }

  .spread-list::-webkit-scrollbar-thumb:hover {
    background: #555;
  }

  .spread-item.updated {
    background-color: rgba(33, 150, 243, 0.1);
  }

  @keyframes highlight {
    from { background-color: rgba(33, 150, 243, 0.2); }
    to { background-color: transparent; }
  }

  .spread-item.highlight {
    animation: highlight 3s ease;
  }

  .loading {
    display: flex;
    justify-content: center;
    align-items: center;
    height: 100px;
    color: #666;
  }
</style> 