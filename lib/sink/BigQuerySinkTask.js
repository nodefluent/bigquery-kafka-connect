"use strict";

const async = require("async");
const { SinkTask } = require("kafka-connect");

class BigQuerySinkTask extends SinkTask {

    start(properties, callback, parentConfig) {

        this.parentConfig = parentConfig;
        this.properties = properties;
        const {
            bigQuery,
            dataset,
            table,
            maxTasks,
            batchSize
        } = this.properties;

        this.bigQuery = bigQuery;
        this.dataset = dataset;
        this.table = table;
        this.batchSize = batchSize;
        this.maxTasks = maxTasks;

        this.initialisedTable = false;
        this.buffer = [];
        this.bufferDraining = false;

        callback(null);
    }

    drainBuffer() {
        if (this.bufferDraining) {
            return Promise.resolve();
        }

        this.bufferDraining = true;

        return new Promise((resolve, reject) => {
            async.whilst(
                () => this.buffer.length !== 0,
                next => this.runBatch().then(() => next()),
                error => {
                    if (error) {
                        return reject(error);
                    }

                    this.bufferDraining = false;
                    resolve();
                }
            );
        });
    }

    runBatch(){

        const rows = this._buffer.splice(0, Math.min(this.batchSize, this.buffer.length));

        return new Promise((resolve, reject) => {
            setTimeout(() => {
                this.table.insert(rows, {raw: true})
                    .then((insertErrors) => {
                        if(insertErrors){
                            return reject(JSON.stringify(insertErrors));
                        }

                        resolve();
                    })
                    .catch(error => reject(error));
            }, INSERT_THRESHOLD);
        });
    }

    beforeFirstPut(record) {

        //schema must be cloned, otherwise the reference would cause serious anomalies
        const clonedSchema = JSON.parse(JSON.stringify(record.valueSchema));

        // TODO: create table (possibly create dataset, maybe move to connector)
        return Promise.resolve();
    }

    putRecords(records) {
        return Promise((resolve, reject) => {

            if (record.value !== null && record.value !== "null") {
                // BigQuery is append only so we'll silently drop deleted entities
                this.parentConfig.emit("model-delete", record.key.toString());
                return resolve();
            }

            records.forEach(record => this.buffer.push({
                record.id,
                record.value
            }));

            if(this.buffer.length >= this.batchSize){
                return this.drainBuffer();
            }

            return Promise.resolve();
        });
    }

    put(records, callback) {

        if (!this.initialisedTable) {
            return this.beforeFirstPut(records[0])
                .then(_ => {
                    this.initialisedTable = true;
                    return this.putRecords(records);
                })
                .then(() => callback(null))
                .catch(error => callback(error));
        }

        this.putRecords(records)
            .then(() => callback(null))
            .catch(error => callback(error));
    }

    stop() {
        //empty (con is closed by connector)
    }
}

module.exports = BigQuerySinkTask;
