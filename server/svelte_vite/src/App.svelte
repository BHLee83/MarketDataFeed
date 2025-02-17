<script>
  import { Router, Link, Route } from "svelte-routing";
  import { fade } from 'svelte/transition';
  import Home from "./routes/Home.svelte";
  import CorrelationMatrixPage from "./routes/CorrelationMatrixPage.svelte";
  import SpreadAnalysisPage from "./routes/SpreadAnalysisPage.svelte";
  import DiversifiedTrendPage from "./routes/DiversifiedTrendPage.svelte";
  
  // URL 기반 라우팅을 위한 현재 URL 가져오기
  export let url = "";
  
  let isMenuOpen = {
    market: false,
    analysis: false,
    technical: false,
    trading: false,
    strategy: false,
    settings: false
  };
  let isDiabled = {
    overview: true
  }
</script>

<Router {url}>
  <nav class="main-nav">
    <div class="nav-content">
      <div class="logo">
        <Link to="/">
          <span class="logo-icon">📈</span>
          <span class="logo-text">Trading<br>System</span>
        </Link>
      </div>
      
      <div class="menu-items">
        <div class="menu-item" 
          role="menuitem"
          tabindex="0"
          on:mouseenter={() => isMenuOpen.market = true}
          on:mouseleave={() => isMenuOpen.market = false}>
          <span>시장</span>
          {#if isMenuOpen.market}
            <div class="submenu" transition:fade={{duration: 100}}>
              <!-- <Link to="/market/overview">개요</Link>
              <Link to="/market/interest">금리</Link>
              <Link to="/market/forex">환율</Link>
              <Link to="/market/indices">지수</Link> -->
              <span class="disabled">개요</span>
              <span class="disabled">지수</span>
              <span class="disabled">금리</span>
              <span class="disabled">환율</span>
            </div>
          {/if}
        </div>

        <div class="menu-item"
          role="menuitem"
          tabindex="0"
          on:mouseenter={() => isMenuOpen.analysis = true}
          on:mouseleave={() => isMenuOpen.analysis = false}>
          <span>분석</span>
          {#if isMenuOpen.analysis}
            <div class="submenu" transition:fade={{duration: 100}}>
              <!-- <Link to="/analysis/fundamental">기본적 분석</Link> -->
              <span class="disabled">기본적 분석 ▶</span>
              <div class="submenu-item"
                role="menuitem"
                tabindex="0"
                on:mouseenter={() => isMenuOpen.technical = true}
                on:mouseleave={() => isMenuOpen.technical = false}>
                <Link to="/analysis/technical">기술적 분석 ▶</Link>
                {#if isMenuOpen.technical}
                  <div class="submenu-horizontal" transition:fade={{duration: 100}}>
                    <Link to="/analysis/spread">스프레드</Link>
                    <Link to="/analysis/correlation">상관 매트릭스</Link>
                  </div>
                {/if}
              </div>
            </div>
          {/if}
        </div>

        <div class="menu-item"
          role="menuitem"
          tabindex="0"
          on:mouseenter={() => isMenuOpen.trading = true}
          on:mouseleave={() => isMenuOpen.trading = false}>
          <span>트레이딩</span>
          {#if isMenuOpen.trading}
            <div class="submenu" transition:fade={{duration: 100}}>
              <div class="submenu-item"
                role="menuitem"
                tabindex="0"
                on:mouseenter={() => isMenuOpen.strategy = true}
                on:mouseleave={() => isMenuOpen.strategy = false}>
                <Link to="/trading/strategy">전략 ▶</Link>
                {#if isMenuOpen.strategy}
                  <div class="submenu-horizontal" transition:fade={{duration: 100}}>
                    <Link to="/strategy/DiversifiedTrend">DiversifiedTrend</Link>
                  </div>
                {/if}
              </div>
              <!-- <Link to="/trading/history">내역</Link> -->
              <span class="disabled">내역</span>
            </div>
          {/if}
        </div>

        <div class="menu-item"
          role="menuitem"
          tabindex="0"
          on:mouseenter={() => isMenuOpen.settings = true}
          on:mouseleave={() => isMenuOpen.settings = false}>
          <span>기타</span>
          {#if isMenuOpen.settings}
            <div class="submenu" transition:fade={{duration: 100}}>
              <!-- <Link to="/settings/account">계정 설정</Link>
              <Link to="/settings/preferences">환경 설정</Link> -->
              <span class="disabled">계정 설정</span>
              <span class="disabled">환경 설정</span>
            </div>
          {/if}
        </div>
      </div>
    </div>
  </nav>

  <main>
    <Route path="/" component={Home} />
    <Route path="/analysis/correlation" component={CorrelationMatrixPage} />
    <Route path="/analysis/spread" component={SpreadAnalysisPage} />
    <!-- <Route path="/trading/strategy" component={StrategyPage} /> -->
    <Route path="/strategy/DiversifiedTrend" component={DiversifiedTrendPage} />
  </main>
</Router>

<style>
  :global(body) {
    margin: 0;
    font-family: 'Pretendard', -apple-system, BlinkMacSystemFont, system-ui, Roboto, sans-serif;
    background-color: #f5f6fa;
    color: #2d3436;
    font-size: 14px;
    line-height: 1.5;
  }

  .main-nav {
    background: white;
    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    position: sticky;
    width: 100%;
    top: 0;
    z-index: 1000;
  }

  .nav-content {
    max-width: 1200px;
    margin: 0 auto;
    padding: 0 20px;
    height: 60px;
    display: flex;
    align-items: right;
    justify-content: space-between;
  }

  .logo :global(a) {
    display: flex;
    align-items: center;
    gap: 10px;
    text-decoration: none;
  }

  .logo-icon {
    font-size: 1.5rem;
    background: linear-gradient(135deg, #00b894, #00cec9);
    background-clip: text; /* 표준 속성 */
    -webkit-background-clip: text; /* 벤더 프리픽스 */
    font-weight: bold;
    transform: translateX(-10px); 
  }

  .logo-text {
    font-size: 0.7rem;
    font-weight: bold;
    line-height: 0.9;
    padding: 20px 0px;
    color: #2d3436;
    background: linear-gradient(135deg, #2d3436, #636e72);
    background-clip: text; /* 표준 속성 */
    -webkit-background-clip: text; /* 벤더 프리픽스 */
    -webkit-text-fill-color: transparent;
    transform: translateX(-20px); 
  }

  .menu-items {
    display: flex;
    gap: 2rem;
  }

  .menu-item {
    position: relative;
    cursor: pointer;
    padding: 20px 0;
  }

  .menu-item span {
    font-weight: 500;
  }

  .submenu {
    position: absolute;
    top: 100%;
    left: 0;
    background: white;
    min-width: 180px;
    box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    border-radius: 4px;
    padding: 8px 0;
  }

  .submenu-item {
    position: relative;
    cursor: pointer;
    padding: 8px 16px;
  }

  .submenu-horizontal {
    position: absolute;
    top: 0;
    left: 100%;
    background: white;
    min-width: 180px;
    box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    border-radius: 4px;
    padding: 8px 0;
  }

  .submenu :global(a) {
    display: block;
    padding: 8px 16px;
    color: #2d3436;
    text-decoration: none;
    transition: background-color 0.2s;
  }

  .submenu span.disabled {
    display: block;
    padding: 8px 16px;
    color: #adadad;
    text-decoration: none;
    transition: background-color 0.2s;
  }

  .submenu :global(a:hover) {
    background-color: #f5f6fa;
  }

  main {
    margin-top: 60px;
    min-height: calc(100vh - 60px);
    padding: 20px;
  }
</style>
