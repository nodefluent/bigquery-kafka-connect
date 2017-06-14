"use strict";

class FakeTable {

    constructor(id, datasetId, projectId) {
        this.id = id;
        this.datasetId = datasetId;
        this.projectId = projectId;

        this._exists = FakeTable.nextValues.exists;
        this._description = JSON.parse(JSON.stringify(FakeTable.nextValues.description));
        this._rows = JSON.parse(JSON.stringify(FakeTable.nextValues.rows));
    }

    create(options, callback) {
        this._exists = true;
        FakeTable.lastCreateOptions = JSON.parse(JSON.stringify(options));
        FakeTable.createCalled = true;

        if (FakeTable.alreadyExistsResponseActive) {
            const message = `Already Exists: Table ${this.projectId}:${this.datasetId}.${this.id}`;
            const errors = [{
                domain: 'global',
                reason: 'duplicate',
                message
            }];
            return callback(
                {
                    stack: "ApiError at some point in the code.",
                    message,
                    code: 409,
                    name: "",
                    errors
                },
                null,
                {
                    error: {
                        errors,
                        code: 409,
                        message
                    }
                }
            );
        }

        return callback(
            null,
            {
                tableReference: {
                    projectId: this.projectId,
                    datasetId: this.datasetId,
                    tableId: this.id
                },
                schema: options.schema,
                timePartitioning: options.timePartitioning
            },
            {});
    }

    exists(callback) {
        return callback(null, this._exists);
    }

    insert(rows, options, callback) {
        FakeTable.lastInsertedRows.push(...rows);
        return callback(null, {raw: options.raw, rowCount: FakeTable.lastInsertedRows.length});
    }

    get(callback) {
        const tableData = this._description;

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

    static setNextDescription(description) {
        FakeTable.nextValues.description = JSON.parse(JSON.stringify(description));
    }

    static setNextRows(rows) {
        FakeTable.nextValues.rows = JSON.parse(JSON.stringify(rows));
    }

    static resetLastInsertedRows() {
        FakeTable.lastInsertedRows = [];
    }

    static resetLastCreateOptions() {
        FakeTable.lastCreateOptions = {};
    }

    static resetCreateCalled() {
        FakeTable.createCalled = false;
    }

    static setAlreadyExistsResponseActive(active) {
        FakeTable.alreadyExistsResponseActive = active;
    }
}

FakeTable.nextValues = {
    exists: true,
    description: {},
    rows: []
};

FakeTable.lastInsertedRows = [];
FakeTable.lastCreateOptions = {};
FakeTable.createCalled = false;
FakeTable.alreadyExistsResponseActive = false;

module.exports = FakeTable;
