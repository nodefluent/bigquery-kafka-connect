"use strict";

class FakeTable {

    constructor(name, datasetName, projectId) {
        this.name = name;
        this.datasetName = datasetName;
        this.projectId = projectId;

        this._exists = FakeTable.nextValues.exists;
        this._schema = JSON.parse(JSON.stringify(FakeTable.nextValues.schema));
        this._rows = JSON.parse(JSON.stringify(FakeTable.nextValues.rows));
    }

    exists(callback) {
        return callback(null, this._exists);
    }

    get(callback) {
        const tableData = {
            metadata: {
                schema: this._schema
            }
        };
        return callback(null, tableData);
    }

    getRows(options, callback) {
        if (options.pageToken && options.pageToken >= this._rows.length) {
            return callback(null, [], {pageToken: this._rows.length}, {});
        }

        const resultRows = JSON.parse(JSON.stringify(this._rows));

        const resultPageStart = options.pageToken || 0;
        const resultPageEnd = Math.min(resultPageStart + (options.maxResults || this._rows.length), this._rows.length);

        const rows = resultRows.slice(resultPageStart, resultPageEnd);
        return callback(null, rows, {pageToken: resultPageEnd}, {});
    }

    static setNextExists(exists) {
        FakeTable.nextValues.exists = exists;
    }

    static setNextSchema(schema) {
        FakeTable.nextValues.schema = JSON.parse(JSON.stringify(schema));
    }

    static setNextRows(rows) {
        FakeTable.nextValues.rows = JSON.parse(JSON.stringify(rows));
    }
}

FakeTable.nextValues = {
    exists: true,
    schema: [],
    rows: []
};

module.exports = FakeTable;
