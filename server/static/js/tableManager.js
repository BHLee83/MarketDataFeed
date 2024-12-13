class TableManager {
    constructor() {
        this.itemRows = {};
        this.table = document.getElementById('tickTable').getElementsByTagName('tbody')[0];
    }

    updateTable(data) {
        data.content.forEach(idx => {
            idx.forEach(tick => {
                this.updateOrCreateRow(data, idx, tick);
            });
        });
    }

    updateOrCreateRow(data, idx, tick) {
        const itemCode = tick.item_code;
        const rowData = [
            new Date(data.timestamp).toLocaleString(),
            itemCode,
            tick.current_price,
            tick.current_vol,
            data.source
        ];

        if (itemCode in itemRows) {
            // 기존 행 업데이트
            const row = table.row[itemRows[itemCode]];
            for (let i = 0; i < rowData.length; i++) {
                row.cells[i].textContent = rowData[i];
            }
        } else {
            // 새로운 행 추가
            const row = table.insertRow(0);
            itemRows[itemCode] = 0;

            // 기존 행들의 인덱스 업데이트
            for (let code in itemRows) {
                if (code === itemCode) {
                    itemRows[code]++;
                }
            }

            // 새 행에 데이터 추가
            rowData.forEach(text => {
                const cell = row.insertCell();
                cell.textContent = text;
            });
        }
    }
} 