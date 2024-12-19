class WebSocketManager {
    constructor() {
        this.connections = {};
        this.tableManager = new TableManager();
        this.chartManager = new ChartManager();
        this.initializeConnections();
    }

    initializeConnections() {
        ['tick', 'minute', 'daily'].forEach(type => {
            this.createConnection(type);
        });
    }

    createConnection(type) {
        const ws = new WebSocket(`ws://${window.location.host}/ws/${type}`);
        
        ws.onopen = () => console.log(`${type} WebSocket 연결됨`);
        
        ws.onmessage = (event) => {
            const data = JSON.parse(event.data);
            this.handleMessage(type, data);
        };
        
        ws.onclose = () => {
            console.log(`${type} WebSocket 연결 종료. 재연결 시도...`);
            setTimeout(() => this.createConnection(type), 1000);
        };
        
        ws.onerror = (err) => {
            console.error(`${type} WebSocket 오류:`, err);
            ws.close();
        };
        
        this.connections[type] = ws;
    }

    handleMessage(type, data) {
        switch(type) {
            case 'tick':
                this.tableManager.updateTable(data);
                break;
            case 'minute':
                this.chartManager.updateMinuteChart(data);
                break;
            case 'daily':
                this.chartManager.updateDailyChart(data);
                break;
        }
    }
}

// 초기화
const wsManager = new WebSocketManager(); 