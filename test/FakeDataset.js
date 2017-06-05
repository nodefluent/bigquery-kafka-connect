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

    create(callback) {
        this._exists = true;
        FakeDataset.createCalled = true;
        return callback(null, {name: this.name}, {});
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
