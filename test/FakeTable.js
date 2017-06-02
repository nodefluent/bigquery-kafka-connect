"use strict";

class FakeTable {

    constructor(name, datasetName, projectId) {
        this.name = name;
        this.datasetName = datasetName;
        this.projectId = projectId;
    }

    exists(callback) {
        return callback(null, true);
    }

    get(callback) {
        const tableData = {
            metadata: {
                schema: {}
            }
        };
        return callback(null, tableData);
    }

    getRows(options, callback) {
        return callback(null, [], null, {});
    }
}

module.exports = FakeTable;
