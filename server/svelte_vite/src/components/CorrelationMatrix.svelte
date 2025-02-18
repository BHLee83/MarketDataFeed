<script>
  import { onMount, onDestroy, createEventDispatcher } from 'svelte';
  
  export let symbols = [];
  export let correlationData = [];  // 배열로 변경
  export let market = null;
  export let timeframe = null;
  
  const dispatch = createEventDispatcher();
  
  function getCorrelation(symbol1, symbol2) {
    if (!correlationData || !Array.isArray(correlationData)) {
        console.error('Invalid correlation data format');
        return null;
    }
    
    if (symbol1 === symbol2) return 1;
    
    const rowIndex = symbols.indexOf(symbol1);
    const colIndex = symbols.indexOf(symbol2);
    
    if (rowIndex === -1 || colIndex === -1) {
        console.error('Symbol not found:', {symbol1, symbol2, symbols});
        return null;
    }
    
    const row = correlationData[rowIndex];
    if (!Array.isArray(row)) {
        console.error('Invalid row data:', row);
        return null;
    }
    
    const value = row[colIndex];
    return typeof value === 'number' ? value : null;
  }

  function handleSymbolClick(symbol) {
    dispatch('matrixClick', {
      clickType: 'symbol',
      symbol1: symbol,
      timeframe
    });
  }

  function handleCorrelationClick(symbol1, symbol2) {
    if (symbol1 === symbol2) return;
    dispatch('matrixClick', {
      clickType: 'correlation',
      symbol1,
      symbol2,
      timeframe
    });
  }

  // 상관계수 표시 셀 컴포넌트
  function formatCorrelation(value) {
    if (value === null || typeof value !== 'number' || isNaN(value)) {
        return '-';
    }
    return value.toFixed(2);
  }
</script>

<div class="correlation-container">
  <table>
    <thead>
      <tr>
        <th></th>
        {#each symbols as symbol}
          <th class="clickable" on:click={() => handleSymbolClick(symbol)}>{symbol}</th>
        {/each}
      </tr>
    </thead>
    <tbody>
      {#each symbols as symbol1}
        <tr>
          <th class="clickable" on:click={() => handleSymbolClick(symbol1)}>{symbol1}</th>
          {#each symbols as symbol2}
            <td class="correlation-cell clickable" 
                on:click={() => symbol1 !== symbol2 && handleCorrelationClick(symbol1, symbol2)}>
              {#if getCorrelation(symbol1, symbol2) === null}
                <span class="loading">-</span>
              {:else}
                <div style="background-color: rgba({getCorrelation(symbol1, symbol2) > 0 ? '0,255,0,' : '255,0,0,'}{Math.abs(getCorrelation(symbol1, symbol2))})">
                  {formatCorrelation(getCorrelation(symbol1, symbol2))}
                </div>
              {/if}
            </td>
          {/each}
        </tr>
      {/each}
    </tbody>
  </table>
</div>

<style>
  .correlation-container {
    margin-bottom: 20px;
    overflow-x: auto;
  }

  table {
    border-collapse: collapse;
    width: 100%;
    background: white;
  }

  th, td {
    border: 1px solid #ddd;
    padding: 8px;
    text-align: center;
  }

  th {
    background-color: #f5f5f5;
    position: sticky;
    top: 0;
    z-index: 1;
  }

  th:first-child {
    position: sticky;
    left: 0;
    z-index: 2;
    background-color: #f5f5f5;
  }

  .correlation-cell {
    transition: background-color 0.3s;
    position: relative;
    min-width: 80px;
  }

  .clickable {
    cursor: pointer;
  }

  .clickable:hover {
    background-color: #f0f0f0;
  }

  .loading {
    color: #666;
    font-style: italic;
    font-size: 0.9em;
  }
</style>