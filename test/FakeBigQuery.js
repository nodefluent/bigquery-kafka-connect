"use strict";

const FakeDataset = require("./FakeDataset");

class FakeBigQuery {

    constructor(options) {
        this.options = options;
    }

    dataset(name) {
        return new FakeDataset(name, this.options.projectId);
    }
}

module.exports = FakeBigQuery;
