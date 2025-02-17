<script>
  import { onMount, onDestroy, createEventDispatcher } from 'svelte';
  
  export let tableData = [];
  export let title = 'Diversified Trend Strategy';
  
  // 이미지 URL을 동적으로 생성
  const VITE_API_URL = import.meta.env.VITE_API_URL;
  const baseImagePath = 'static/lib/images/';
  const heatmapUrl = `${VITE_API_URL}/${baseImagePath}DT_heatmap.png`;
  const dendrogramUrl = `${VITE_API_URL}/${baseImagePath}DT_dendrogram.png`;
  
  // 이미지 로딩 상태 관리
  let heatmapLoaded = false;
  let dendrogramLoaded = false;
  
  const dispatch = createEventDispatcher();
  
  function handleRowClick(item) {
    dispatch('tableClick', {
      clickType: 'row',
      item
    });
  }

  function handleImageClick(imageName) {
    dispatch('imageClick', {
      image: imageName
    });
  }

  // 이미지 로딩 실패 처리
  function handleImageError(imageType) {
    console.error(`Failed to load ${imageType} image`);
    // 필요한 경우 에러 상태 처리
  }
</script>

<div class="page-container">
  <h2>{title}</h2>
  
  <div class="content-wrapper">
    <div class="table-container">
      <!-- 테이블 코드 -->
    </div>

    <div class="images-container">
      <div class="image-wrapper">
        <button 
          type="button" 
          class="image-button" 
          on:click={() => handleImageClick('heatmap')}
          on:keydown={(e) => e.key === 'Enter' && handleImageClick('heatmap')}
          aria-label="히트맵 상세보기"
        >
          {#if !heatmapLoaded}
            <div class="loading">이미지 로딩 중...</div>
          {/if}
          <img 
            src={heatmapUrl} 
            alt="" 
            on:load={() => heatmapLoaded = true}
            on:error={() => handleImageError('heatmap')}
            class:hidden={!heatmapLoaded}
          />
        </button>
        <p>Heatmap</p>
      </div>

      <div class="image-wrapper">
        <button 
          type="button" 
          class="image-button" 
          on:click={() => handleImageClick('dendrogram')}
          on:keydown={(e) => e.key === 'Enter' && handleImageClick('dendrogram')}
          aria-label="덴드로그램 상세보기"
        >
          {#if !dendrogramLoaded}
            <div class="loading">이미지 로딩 중...</div>
          {/if}
          <img 
            src={dendrogramUrl} 
            alt="" 
            on:load={() => dendrogramLoaded = true}
            on:error={() => handleImageError('dendrogram')}
            class:hidden={!dendrogramLoaded}
          />
        </button>
        <p>Dendrogram</p>
      </div>
    </div>
  </div>
</div>

<style>
  /* 기존 스타일 유지 */
  
  .hidden {
    display: none;
  }

  .loading {
    padding: 20px;
    background: #f5f5f5;
    border-radius: 8px;
    color: #666;
    font-style: italic;
  }

  .images-container {
    display: flex;
    flex-direction: column;
    gap: 20px;
  }

  .image-wrapper {
    text-align: center;
  }

  .image-wrapper p {
    margin-top: 8px;
    font-weight: bold;
  }

  .image-button {
    width: 100%;
    padding: 0;
    border: none;
    background: none;
    cursor: pointer;
    transition: all 0.3s ease;
  }

  .image-button:hover {
    opacity: 0.9;
  }

  .image-button:focus {
    outline: 2px solid #007bff;
    outline-offset: 2px;
  }

  img {
    max-width: 100%;
    height: auto;
    border-radius: 8px;
    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
  }
</style>