"use strict";

const FakeTable = require("./FakeTable");

class FakeDataset {

    constructor(id, projectId) {
        this.id = id;
        this.projectId = projectId;

        this._exists = FakeDataset.nextValues.exists;
    }

    table(id) {
        return new FakeTable(id, this.id, this.projectId);
    }

    create(callback) {
        this._exists = true;
        FakeDataset.createCalled = true;
        return callback(null, {id: this.id}, {});
    }

    exists(callback) {
        return callback(null, this._exists);
    }

    static setNextExists(exists) {
        FakeDataset.nextValues.exists = exists;
    }

    static resetCreateCalled() {
        FakeDataset.createCalled = false;
    }
}

FakeDataset.nextValues = {
    exists: true
};

FakeDataset.createCalled = false;

module.exports = FakeDataset;
