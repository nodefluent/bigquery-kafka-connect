"use strict";

const assert = require("assert");
const testdouble = require("testdouble");
const FakeBigQuery = require("../FakeBigQuery");
const FakeDataset = require("../FakeDataset");
const FakeTable = require("../FakeTable");
testdouble.replace("@google-cloud/bigquery", FakeBigQuery);

const { SourceRecord } = require("kafka-connect");
const { runSourceConnector, runSinkConnector, ConverterFactory } = require("./../../index.js");
const sourceProperties = require("./../source-config.js");
const sinkProperties = require("./../sink-config.js");

describe("Connector INT", () => {

    const bigQuerySchema = {
        fields: [
            { name: "id", type: "INTEGER", mode: "REQUIRED" },
            { name: "name", type: "STRING", mode: "REQUIRED" },
            { name: "info", type: "STRING", mode: "NULLABLE" }
        ]
    };

    const jsonSchema = {
        type: "object",
        required: ["fields"],
        properties: {
            fields: {
                type: "array",
                items: {
                    type: "object",
                    required: ["name", "type", "mode"],
                    properties: {
                        name: {type: "string"},
                        type: {type: "string"},
                        mode: {type: ["string", "null"]}
                    }
                }
            }
        }
    };

    describe("Source connects and streams", () => {

        let config = null;
        let error = null;
        let rows = null;

        before("Setup BigQuery fake", () => {

            rows = [
                {
                    id: 1,
                    name: "Item No. 1",
                    info: "Item Information"
                },
                {
                    id: 2,
                    name: "Item No. 2",
                    info: null
                },
                {
                    id: 3,
                    name: "Item No. 3",
                    info: "Item Information"
                }
            ];

            FakeTable.setNextSchema(bigQuerySchema);
            FakeTable.setNextRows(rows);
        });

        it("should be able to run BigQuery source config", () => {
            const onError = _error => {
                error = _error;
            };
            return runSourceConnector(sourceProperties, [], onError).then(_config => {
                config = _config;
                config.on("record-read", id => console.log("read: " + id));
                return true;
            });
        });

        it("should be able to await a few pollings", done => {
            setTimeout(() => {
                assert.ifError(error);
                done();
            }, 4500);
        });

        it("should be able to fake a delete action", () => {

            const record = new SourceRecord();
            record.key = "1";
            record.value = null; //will cause this record to be deleted when read by sink-task

            return config.produce(record);
        });

        it("should be able to close configuration", done => {
            config.stop();
            setTimeout(done, 1500);
        });
    });

    describe("Source dataset doesn't exist", () => {

        before("Setup BigQuery fake", () => {
            FakeDataset.setNextExists(false);
        });

        it("should not be able to run BigQuery source config", done => {
            const onError = _error => {
                error = _error;
            };

            runSourceConnector(sourceProperties, [], onError)
                .then(_ => {
                    done("The source connector ran when it shouldn't.");
                })
                .catch(_error => {
                    assert.equal(_error.message, "The specified dataset doesn't exist.");
                    done();
                });
        });
    });

    describe("Source table doesn't exist", () => {

        before("Setup BigQuery fake", () => {
            FakeDataset.setNextExists(true);
            FakeTable.setNextExists(false);
        });

        it("should not be able to run BigQuery source config", done => {
            const onError = _error => {
                error = _error;
            };

            runSourceConnector(sourceProperties, [], onError)
                .then(_ => {
                    done("The source connector ran when it shouldn't.");
                })
                .catch(_error => {
                    assert.equal(_error.message, "The specified table doesn't exist.");
                    done();
                });
        });
    });

    describe("Sink connects, creates dataset and table and streams", () => {

        before("Setup BigQuery fake", () => {
            FakeDataset.setNextExists(false);
            FakeDataset.resetCreateCalled();
            FakeTable.setNextExists(false);
            FakeTable.resetLastInsertedRows();
            FakeTable.resetCreateCalled();
            FakeTable.resetLastCreateSchema();
        });

        let config = null;
        let error = null;

        it("should be able to run the BigQuery sink config", () => {
            const onError = _error => {
                error = _error;
            };
            return runSinkConnector(sinkProperties, [], onError).then(_config => {
                config = _config;
                config.on("model-upsert", id => console.log("upsert: " + id));
                config.on("model-delete", id => console.log("delete: " + id));
                return true;
            });
        });

        it("should be able to await a few message puts", done => {
            setTimeout(() => {
                assert.ifError(error);
                done();
            }, 4500);
        });

        it("should be able to close configuration", done => {
            config.stop();
            setTimeout(done, 1500);
        });

        it("should have created the dataset", () => {
            assert.ok(FakeDataset.createCalled);
        });

        it("should have created the table", () => {
            assert.ok(FakeTable.createCalled);
            assert.deepEqual(FakeTable.lastCreateSchema, bigQuerySchema);
        });

        it("should be able to see table data", () => {
            assert.equal(FakeTable.lastInsertedRows.length, 3);
        });
    });
});
