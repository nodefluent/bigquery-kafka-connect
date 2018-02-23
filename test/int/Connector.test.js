"use strict";

const assert = require("assert");
const testdouble = require("testdouble");
const FakeBigQuery = require("../FakeBigQuery");
const FakeDataset = require("../FakeDataset");
const FakeTable = require("../FakeTable");
testdouble.replace("@google-cloud/bigquery", FakeBigQuery);

const uuid = require("uuid");
const {Producer} = require("sinek");
const {SourceRecord} = require("kafka-connect");
const {runSourceConnector, runSinkConnector, ConverterFactory} = require("./../../index.js");

const sourceProperties = require("./../source-config.js");
const sinkProperties = require("./../sink-config.js");

describe("Connector INT", () => {

    const bigQueryTableDescription = {
        schema: {
            fields: [
                {name: "id", type: "INTEGER", mode: "REQUIRED"},
                {name: "name", type: "STRING", mode: "REQUIRED"},
                {name: "info", type: "STRING", mode: "NULLABLE"}
            ]
        },
        timePartitioning: {type: "DAY"}
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

            FakeTable.setNextDescription(bigQueryTableDescription);
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
                    done(new Error("The source connector ran when it shouldn't"));
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
                    done(new Error("The source connector ran when it shouldn't"));
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
            FakeTable.resetLastCreateOptions();
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
            assert.deepEqual(FakeTable.lastCreateOptions, bigQueryTableDescription);
        });

        it("should be able to see the inserted rows", () => {
            assert.equal(FakeTable.lastInsertedRows.length, 3);
        });
    });

    describe("Sink connects, inserts, retries on error", () => {

        let config = null;
        let error = null;
        let topic = "kc_bigquery_test_retry" + uuid.v4();
        let producer = null;

        before("Setup BigQuery fake", () => {
            FakeDataset.resetCreateCalled();
            FakeTable.resetLastInsertedRows();
            FakeTable.setErrorOnNextInsert()
        });

        before("Produce 4 messages", () => {
            producer = new Producer(sinkProperties.kafka, topic, 1);
            return producer.connect().then(_ => {
                return Promise.all([
                    producer.buffer(topic, "10", {payload: {id: 10, name: "test10", info: null}, type: "publish"}),
                    producer.buffer(topic, "11", {payload: {id: 11, name: "test11", info: null}, type: "publish"}),
                    producer.buffer(topic, "12", {payload: {id: 12, name: "test12", info: null}, type: "publish"}),
                    producer.buffer(topic, "13", {payload: {id: 13, name: "test13", info: null}, type: "publish"})
                ]);
            });
        });

        it("should be able to run the BigQuery sink config", () => {
            const sinkPropertiesRetry = sinkProperties;
            sinkPropertiesRetry.topic = topic;
            sinkPropertiesRetry.connector.batchSize = 2;
            const etlFunc = (messageValue, callback) => {
                return callback(null, messageValue.payload);
            };
            let converter = ConverterFactory.createSinkSchemaConverter({}, etlFunc);

            return runSinkConnector(sinkPropertiesRetry, [converter]).then(_config => {
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

        it("should be able to see the inserted rows", () => {
            assert.equal(FakeTable.lastInsertedRows.length, 4);
        });
    });

    describe("Sink swallows table exists errors, if multiple instances try to create it concurrently", () => {

        before("Setup BigQuery fake", () => {
            FakeDataset.setNextExists(true);
            FakeTable.setNextExists(false);
            FakeTable.resetLastInsertedRows();
            FakeTable.resetCreateCalled();
            FakeTable.resetLastCreateOptions();
            FakeTable.setAlreadyExistsResponseActive(true);
        });

        let config = null;
        let error = null;

        it("should be able to produce a message", () => {
            const producer = new Producer(sinkProperties.kafka, sinkProperties.topic, 1);
            return producer.connect()
                .then(() => producer.buffer(sinkProperties.topic, "3", {payload: {id: 9, name: "dummy", info: "for swallow test"}, type: "publish"}));
        });

        it("should be able to await message publication", done => {
            setTimeout(() => {
                assert.ifError(error);
                done();
            }, 1500);
        });

        it("should be able to run the BigQuery sink config", () => {
            const converter = ConverterFactory.createSinkSchemaConverter(bigQueryTableDescription, (messageValue, callback) => callback(null, messageValue.payload));

            const onError = _error => {
                error = _error;
            };
            return runSinkConnector(sinkProperties, [converter], onError).then(_config => {
                config = _config;
                return true;
            });
        });

        it("should be able to await sink connection", done => {
            setTimeout(() => {
                assert.ifError(error);
                done();
            }, 4500);
        });

        it("should be able to close configuration", done => {
            config.stop();
            setTimeout(done, 1500);
        });

        it("should have created the table", () => {
            assert.ok(FakeTable.createCalled);
            assert.deepEqual(FakeTable.lastCreateOptions, bigQueryTableDescription);
        });

        it("should have swallowed the error if the table already existed", () => {
            assert.ifError(error);
        });
    });

    describe("Converter Factory", () => {

        let config = null;
        let error = null;
        let topic = "kc_bigquery_test_cf";
        let converter = {};
        let producer = null;

        before("Setup the BigQueryFake", () => {
            FakeDataset.setNextExists(true);
            FakeTable.resetLastInsertedRows();
            FakeTable.setNextExists(true);
            FakeTable.setAlreadyExistsResponseActive(false);
        });

        it("should be able to create a custom converter", done => {

            const etlFunc = (messageValue, callback) => {

                //type is an example json format field
                if (messageValue.type === "publish") {
                    return callback(null, {
                        id: messageValue.payload.id,
                        name: messageValue.payload.name,
                        info: messageValue.payload.info
                    });
                }

                if (messageValue.type === "unpublish") {
                    return callback(null, null); //null value will cause deletion
                }

                console.log(messageValue);
                throw new Error("unknown messageValue.type");
            };

            converter = ConverterFactory.createSinkSchemaConverter(bigQueryTableDescription, etlFunc);

            const payload = {
                id: 1,
                name: "The first item",
                info: "Give me a description, please!"
            };

            const aFakeKafkaMessage = {
                partition: 0,
                topic: "test",
                value: {
                    payload,
                    type: "publish"
                },
                offset: 1,
                key: "1"
            };

            converter.toConnectData(Object.assign({}, aFakeKafkaMessage), (error, message) => {

                assert.ifError(error);
                assert.deepEqual(message.value.valueSchema, bigQueryTableDescription);
                assert.deepEqual(message.value.value, payload);
                assert.ok(message.key);
                assert.ok(message.value.key);

                converter.toConnectData(Object.assign({}, aFakeKafkaMessage), (error, message) => {

                    assert.ifError(error);
                    assert.deepEqual(message.value.valueSchema, bigQueryTableDescription);
                    assert.deepEqual(message.value.value, payload);
                    assert.ok(message.key);
                    assert.ok(message.value.key);

                    done();
                });
            });
        });

        it("should be able to produce a few messages", () => {
            producer = new Producer(sinkProperties.kafka, topic, 1);
            return producer.connect().then(_ => {
                return Promise.all([
                    producer.buffer(topic, "3", {payload: {id: 3, name: "test1", info: null}, type: "publish"}),
                    producer.buffer(topic, "4", {payload: {id: 4, name: "test2", info: null}, type: "publish"}),
                    producer.buffer(topic, "3", {payload: null, type: "unpublish"})
                ]);
            });
        });

        it("should be able to await a few broker interactions", done => {
            setTimeout(() => {
                assert.ifError(error);
                done();
            }, 1500);
        });

        it("shoud be able to sink message through custom converter", () => {
            const onError = _error => {
                error = _error;
            };

            const customProperties = Object.assign({}, sinkProperties, {topic});
            return runSinkConnector(customProperties, [converter], onError).then(_config => {
                config = _config;
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
            producer.close();
            setTimeout(done, 1500);
        });

        it("should be able to see the inserted rows", () => {
            assert.equal(FakeTable.lastInsertedRows.length, 2);
            assert.deepEqual(FakeTable.lastInsertedRows, [
                {insertId: "3", json: {id: 3, name: "test1", info: null}},
                {insertId: "4", json: {id: 4, name: "test2", info: null}}
            ]);
        });
    });
});
