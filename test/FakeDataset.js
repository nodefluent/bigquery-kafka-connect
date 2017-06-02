"use strict";

const FakeTable = require("./FakeTable");

class FakeDataset {

    constructor(name, projectId) {
        this.name = name;
        this.projectId = projectId;
    }

    table(name) {
        return new FakeTable(name, this.name, this.projectId);
    }

    exists(callback) {
        return callback(null, true);
    }
}

module.exports = FakeDataset;
