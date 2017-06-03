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

describe("Connector INT", () => {

    describe("Source connects and streams", () => {

        let config = null;
        let error = null;
        let schema = null;
        let rows = null;

        before("Setup BigQuery fake", () => {

            schema = {
                fields: [
                    { name: 'id', type: 'INTEGER', mode: 'REQUIRED' },
                    { name: 'name', type: 'STRING', mode: 'REQUIRED' },
                    { name: 'info', type: 'STRING', mode: 'NULLABLE' }
                ]
            };

            rows = [
                {
                    id: 1,
                    name: "Item No. 1",
                    info: "Item Information"
                },
                {
                    id: 2,
                    name: "Item No. 2"
                },
                {
                    id: 3,
                    name: "Item No. 3",
                    info: "Item Information"
                }
            ];

            FakeTable.setNextSchema(schema);
            FakeTable.setNextRows(rows);
        });

        it("should be able to run BigQuery source config", function() {
            const onError = _error => {
                error = _error;
            };
            return runSourceConnector(sourceProperties, [], onError).then(_config => {
                config = _config;
                config.on("record-read", id => console.log("read: " + id));
                return true;
            });
        });

        it("should be able to await a few pollings", function(done) {
            setTimeout(() => {
                assert.ifError(error);
                done();
            }, 4500);
        });

        it("should be able to fake a delete action", function() {

            const record = new SourceRecord();
            record.key = "1";
            record.value = null; //will cause this record to be deleted when read by sink-task

            return config.produce(record);
        });

        it("should be able to close configuration", function(done) {
            config.stop();
            setTimeout(done, 1500);
        });
    });

    describe("Source dataset doesn't exist", () => {

        before("Setup BigQuery fake", () => {
            FakeDataset.setNextExists(false);
        });

        it("should not be able to run BigQuery source config", function(done) {
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

        it("should not be able to run BigQuery source config", function(done) {
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

    describe("Sink connects and streams", function() {
        // TODO
    });
});
