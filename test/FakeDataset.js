"use strict";

const FakeTable = require("./FakeTable");

class FakeDataset {

    constructor(name, projectId) {
        this.name = name;
        this.projectId = projectId;

        this._exists = FakeDataset.nextValues.exists;
    }

    table(name) {
        return new FakeTable(name, this.name, this.projectId);
    }

    exists(callback) {
        return callback(null, this._exists);
    }

    static setNextExists(exists) {
        FakeDataset.nextValues.exists = exists;
    }
}

FakeDataset.nextValues = {
    exists: true
};

module.exports = FakeDataset;
