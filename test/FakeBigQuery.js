"use strict";

const FakeDataset = require("./FakeDataset");

class FakeBigQuery {

    constructor(options) {
        this.options = options;
    }

    dataset(id) {
        return new FakeDataset(id, this.options.projectId);
    }
}

module.exports = FakeBigQuery;
