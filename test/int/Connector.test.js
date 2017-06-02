"use strict";

const assert = require("assert");

const { SourceRecord } = require("kafka-connect");
const { runSourceConnector, runSinkConnector, ConverterFactory } = require("./../../index.js");
const sourceProperties = require("./../source-config.js");

describe("Connector INT", () => {

    describe("Source", () => {

        let config = null;
        let error = null;

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

    describe("Sink", () => {
        // TODO
    });
});
