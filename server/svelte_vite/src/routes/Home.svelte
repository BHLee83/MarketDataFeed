<script>
  import { onMount } from 'svelte';
  
  let marketSummary = {
    kospi: { value: 0, change: 0 },
    kosdaq: { value: 0, change: 0 }
  };
  
  onMount(async () => {
    // 실제 데이터를 가져오는 API 호출
    // try {
    //   const data = await fetch('/api/market-summary').then((res) => res.json());
    //   marketSummary = data || marketSummary;
    // } catch (error) {
    //   console.error('Failed to fetch market summary:', error);
    // }
    // 임시 데이터로 대체
    marketSummary = {
      kospi: { value: 0.0, change: 0.0 },
      kosdaq: { value: 0.0, change: 0.0 }
    };
  });
</script>

<div class="home-container">
  <section class="hero">
    <h1>TradingSystem</h1>
    <p class="subtitle">나만의 시장 분석 플랫폼</p>
  </section>

  <section class="market-summary">
    <div class="market-card">
      <h3>KR KTB 10Y</h3>
      <div class="market-value">
        <span class="value">{marketSummary.kospi?.value?.toFixed(2) || '0.0'}</span>
        <span class={marketSummary.kospi?.change >= 0 ? 'change positive' : 'change negative'}>
          {marketSummary.kospi?.change >= 0 ? '+' : ''}{marketSummary.kospi?.change}%
        </span>
      </div>
    </div>

    <div class="market-card">
      <h3>US Tresury 10Y</h3>
      <div class="market-value">
        <span class="value">{marketSummary.kosdaq?.value?.toFixed(2) || '0.0'}</span>
        <span class={marketSummary.kosdaq?.change >= 0 ? 'change positive' : 'change negative'}>
          {marketSummary.kosdaq?.change >= 0 ? '+' : ''}{marketSummary.kosdaq?.change}%
        </span>
      </div>
    </div>
  </section>

  <section class="features">
    <h2>새로운 기능</h2>
    <div class="feature-grid">
      <div class="feature-card">
        <div class="feature-icon">📊</div>
        <div class="feature-content">
          <h3>스프레드 분석</h3>
          <p>상품간 가격차를 통한 시장 분석</p>
        </div>
      </div>
      <div class="feature-card">
        <div class="feature-icon">🎢</div>
        <div class="feature-content">
          <h3>상관관계 분석</h3>
          <p>주요 종목 간 상관관계 분석 및 시각화</p>
        </div>
      </div>
    </div>
  </section>
</div>

<style>
  .home-container {
    max-width: 1400px;
    margin: 0 auto;
    padding: 2rem;
  }

  .hero {
    text-align: center;
    margin-bottom: 3rem;
  }

  .hero h1 {
    font-size: 2.5rem;
    font-weight: 700;
    margin-bottom: 0.5rem;
    color: #2d3436;
    text-shadow: 
    2px 2px 2px rgba(0, 0, 0, 0.2), /* 부드러운 그림자 */
    4px 4px 4px rgba(0, 0, 0, 0.1); /* 더 멀리 퍼지는 그림자 */
  }
  
  .subtitle {
    font-size: 1.2rem;
    color: #636e72;
  }

  .market-summary {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
    gap: 1.5rem;
    margin-bottom: 3rem;
  }

  .market-card {
    background: white;
    padding: 1.5rem;
    border-radius: 8px;
    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
  }

  .market-card h3 {
    margin: 0 0 1rem 0;
    color: #2d3436;
  }

  .market-value {
    display: flex;
    align-items: baseline;
    gap: 1rem;
  }

  .value {
    font-size: 1.5rem;
    font-weight: 600;
  }

  .change {
    font-size: 1rem;
    font-weight: 500;
  }

  .positive { color: #00b894; }
  .negative { color: #d63031; }

  .features {
    margin-top: 3rem;
  }

  .features h2 {
    text-align: center;
    margin-bottom: 2rem;
    color: #2d3436;
  }

  .feature-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
    gap: 1.5rem;
  }

  .feature-card {
    background: white;
    padding: 1.5rem;
    border-radius: 8px;
    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    display: flex;
    align-items: center;
    gap: 1rem;
    transition: transform 0.2s ease-in-out;
  }

  .feature-card:hover {
    transform: translateY(-2px);
  }

  .feature-icon {
    font-size: 2rem;
    width: 50px;
    height: 50px;
    display: flex;
    align-items: center;
    justify-content: center;
    background: linear-gradient(135deg, #f5f6fa, #dfe6e9);
    border-radius: 12px;
  }

  .feature-content {
    flex: 1;
  }

  .feature-content h3 {
    margin: 0 0 0.5rem 0;
    color: #2d3436;
  }

  .feature-content p {
    margin: 0;
    color: #636e72;
  }
</style> 