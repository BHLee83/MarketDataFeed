class TableManager {
    constructor() {
        this.itemRows = {};
        this.table = document.getElementById('tickTable').getElementsByTagName('tbody')[0];
    }

    updateTable(data) {
        data.forEach(idx => {
            idx.content.forEach(tick => {
                this.updateOrCreateRow(idx, tick);
            });
        });
    }

    updateOrCreateRow(idx, tick) {
        // ... existing table update logic ...
    }
} 