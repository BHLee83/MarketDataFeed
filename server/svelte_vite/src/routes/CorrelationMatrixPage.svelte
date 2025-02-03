<script>
  import { onMount, onDestroy } from 'svelte';
  import { memoryStoreStatus } from '../lib/store.js';
  import { dbStore, fetchStatisticsData, getLastDate } from '../lib/db.js';
  import { realtimeStore, connectStatisticsWebSocket } from '../lib/realtime.js';
  import CorrelationMatrix from '../components/CorrelationMatrix.svelte';

  let markets = ['EQ', 'IR', 'FX', 'CM', 'ECO'];
  let isLoading = false;
  let error = null;

  // 마켓별 상태 관리를 반응형으로 선언
  $: marketStates = markets.reduce((acc, market) => {
    if (!acc[market]) {
      acc[market] = {
        selectedTimeframe: '1d',
        selectedDateRange: '1d',
        selectedPeriod: '30',
        correlationData: [],
        symbols: [],
        isLoading: false,
      };
    }
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
      // 로딩 상태 설정
      marketStates = {
        ...marketStates,
        [market]: {
          ...marketStates[market],
          isLoading: true
        }
      };

      const { start, end } = await calculateDateRange(marketStates[market].selectedDateRange);
      
      const data = await fetchStatisticsData(
        `corr_${marketStates[market].selectedPeriod}`,
        marketStates[market].selectedTimeframe,
        market,
        null,
        start,
        end
      );
      
      if (data) {
        const symbolSet = new Set();
        const correlationMap = new Map();

        Object.entries(data).forEach(([key, correlations]) => {
          const [symbol1, symbol2] = key.split('-');
          symbolSet.add(symbol1);
          symbolSet.add(symbol2);

          if (Array.isArray(correlations) && correlations.length > 0) {
            const lastCorrelation = correlations[correlations.length - 1];
            if (lastCorrelation && typeof lastCorrelation.value1 !== 'undefined') {
              correlationMap.set(key, lastCorrelation.value1);
            }
          }
        });

        const symbols = Array.from(symbolSet).sort();
        const correlationData = symbols.map(symbol1 => {
          return symbols.map(symbol2 => {
            if (symbol1 === symbol2) return 1;
            const key = [symbol1, symbol2].sort().join('-');
            return correlationMap.get(key) ?? null;
          });
        });

        // 상태 직접 업데이트
        marketStates = {
          ...marketStates,
          [market]: {
            ...marketStates[market],
            symbols,
            correlationData,
            isLoading: false
          }
        };
      }
      
    } catch (err) {
      error = err.message;
      console.error(`[${market}] 데이터 로딩 에러:`, err);
    } finally {
      // 로딩 상태 해제
      marketStates = {
        ...marketStates,
        [market]: {
          ...marketStates[market],
          isLoading: false
        }
      };
    }
  }

  // 실시간 데이터 업데이트
  function updateCorrelationData(market, data) {
    const state = marketStates[market];
    if (data.type !== `corr_${state.selectedPeriod}`) return;

    const symbolIndex1 = state.symbols.indexOf(data.symbol1);
    const symbolIndex2 = state.symbols.indexOf(data.symbol2);
    
    if (symbolIndex1 >= 0 && symbolIndex2 >= 0) {
      state.correlationData[symbolIndex1][symbolIndex2] = data.value1;
      state.correlationData[symbolIndex2][symbolIndex1] = data.value1;
      marketStates[market] = { ...state };  // Svelte 반응성 트리거
    }
  }

  // 필터 변경 처리
  async function handleFilterChange(market) {
    await loadMarketData(market);
  }

  // WebSocket 설정
  function setupWebSocket() {
    if (statisticsWs) {
      statisticsWs.close();
    }
    statisticsWs = connectStatisticsWebSocket('correlation');
    
    const unsubscribe = realtimeStore.subscribe(storeState => {
      const key = `statistics-correlation`;
      const realtimeData = storeState.statisticsData.get(key);
      if (realtimeData && realtimeData.length > 0) {
        const lastData = realtimeData[realtimeData.length - 1];
        updateCorrelationData(lastData.market, lastData);
      }
    });

    return unsubscribe;
  }

  onMount(() => {
    const unsubscribe = memoryStoreStatus.subscribe(status => {
      isLoading = status.isLoading;
    });

    (async () => {
      for (const market of markets) {
        await loadMarketData(market);
      }
      setupWebSocket();
    })();

    return () => {
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
            on:change={() => handleFilterChange(market)}
          >
            <option value="1m">1분</option>
            <option value="3m">3분</option>
            <option value="5m">5분</option>
            <option value="10m">10분</option>
            <option value="15m">15분</option>
            <option value="30m">30분</option>
            <option value="1d">1일</option>
          </select>

          <select 
            bind:value={marketStates[market].selectedPeriod}
            disabled={marketStates[market].isLoading}
            on:change={() => handleFilterChange(market)}
          >
            <option value="30">30일</option>
            <option value="120">120일</option>
          </select>

          <select 
            bind:value={marketStates[market].selectedDateRange}
            disabled={marketStates[market].isLoading}
            on:change={() => handleFilterChange(market)}
          >
            <option value="today">오늘</option>
            <option value="1d">1일</option>
            <option value="1w">1주</option>
            <option value="1m">1달</option>
          </select>
        </div>
      </div>

      {#if marketStates[market].isLoading}
        <div class="loading">데이터를 불러오는 중...</div>
      {:else if error}
        <div class="error">{error}</div>
      {:else if marketStates[market].correlationData?.length > 0}
        <div class="matrix-container">
          <CorrelationMatrix 
            correlationData={marketStates[market].correlationData}
            symbols={marketStates[market].symbols}
            market={market}
            timeframe={marketStates[market].selectedTimeframe}
          />
        </div>
      {:else}
        <div class="no-data">데이터가 없습니다.</div>
      {/if}
    </div>
  {/each}
</div>

<style>
  .markets {
    display: flex;
    flex-wrap: wrap;
    gap: 20px;
    padding: 20px;
  }

  .market-section {
    flex: 1;
    min-width: 400px;
    border: 1px solid #ddd;
    border-radius: 8px;
    padding: 15px;
  }

  .market-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 15px;
  }

  .controls {
    display: flex;
    gap: 10px;
  }

  select {
    padding: 4px 8px;
    border: 1px solid #ddd;
    border-radius: 4px;
  }

  .matrix-container {
    height: 400px;
    overflow: auto;
  }

  .loading, .error {
    text-align: center;
    padding: 20px;
    color: #666;
  }

  .error {
    color: #f44336;
  }

  h3 {
    margin: 0;
    color: #333;
  }
</style> 