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
            batchSize,
            idColumn
        } = this.properties;

        this.bigQuery = bigQuery;
        this.dataset = dataset;
        this.table = table;
        this.batchSize = batchSize;
        this.maxTasks = maxTasks;

        this.initializedTable = false;
        this.buffer = [];
        this.bufferDraining = false;

        async.series(
            [
                done => this._ensureDatasetExists(done),
                done => this._checkTableExists(done)
            ],
            callback
        );
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

        const rows = this.buffer.splice(0, Math.min(this.batchSize, this.buffer.length));

        return new Promise((resolve, reject) => this.table.insert(
            rows,
            {raw: true},
            (error, apiResponse) => {
                if(error){
                    return reject(JSON.stringify(error));
                }

                resolve();
            }));
    }

    beforeFirstPut(record) {

        //schema must be cloned, otherwise the reference would cause serious anomalies
        const clonedSchema = JSON.parse(JSON.stringify(record.valueSchema));

        return new Promise((resolve, reject) => this.table.create(
            {schema: clonedSchema},
            (error, table, apiResponse) => {
                if(error) {
                    return reject(JSON.stringify(error));
                }

                resolve();
            }
        ));
    }

    putRecords(records) {
        return new Promise((resolve, reject) => {

            records.forEach(record => {

                if (record.value === null || record.value === "null") {
                    // BigQuery is append only so we'll silently drop deleted entities
                    this.parentConfig.emit("model-delete", record.key.toString());
                    return;
                }

                this.parentConfig.emit("model-upsert", record.key.toString());
                this.buffer.push({
                    insertId: record.value[this.properties.idColumn].toString(),
                    json: record.value
                });
            });

            if(this.buffer.length >= this.batchSize){
                return this.drainBuffer()
                    .then(() => resolve())
                    .catch(error => reject(error));
            }

            resolve();
        });
    }

    put(records, callback) {

        if (!this.initializedTable) {
            return this.beforeFirstPut(records[0])
                .then(_ => {
                    this.initializedTable = true;
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

    _ensureDatasetExists(callback) {
        this.dataset.exists((error, exists) => {
            if(error) {
                return callback(new Error(`Failed to check if dataset exists: ${JSON.stringfy(error)}`));
            }

            if(!exists) {
                return this.dataset.create(callback);
            }

            callback();
        });
    }

    _checkTableExists(callback) {
        this.table.exists((error, exists) => {
            if(error) {
                return callback(new Error(`Failed to check if table exists: ${JSON.stringfy(error)}`));
            }

            this.initializedTable = exists;
            callback();
        });
    }
}

module.exports = BigQuerySinkTask;
