class WebSocketManager {
    constructor() {
        this.ws = null;
        this.tableManager = new TableManager();
        this.chartManager = new ChartManager();
    }

    connect() {
        this.ws = new WebSocket(`ws://${window.location.host}/ws/market-data`);
        this.setupEventHandlers();
    }

    setupEventHandlers() {
        this.ws.onopen = () => console.log('WebSocket 연결됨');
        this.ws.onmessage = (event) => this.handleMessage(event);
        this.ws.onclose = () => this.handleClose();
        this.ws.onerror = (err) => this.handleError(err);
    }

    handleMessage(event) {
        const data = JSON.parse(event.data);
        
        switch(data.type) {
            case 'tick_data':
                this.tableManager.updateTable(data.data);
                break;
            case 'minute_data':
                this.chartManager.updateMinuteChart(data);
                break;
            // ... other cases
        }
    }

    // ... other methods
}

// Initialize on load
const wsManager = new WebSocketManager();
wsManager.connect(); 