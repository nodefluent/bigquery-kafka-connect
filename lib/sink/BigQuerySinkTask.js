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
        this.idColumn = idColumn;

        this.initializedTable = false;
        this.buffer = [];
        this.bufferDraining = false;

        this._stats = {
            tableCreated: false,
            batchRuns: 0,
            rowsInserted: 0,
            insertErrors: 0
        }

        this.parentConfig.on("get-stats", () => {
            this.parentConfig.emit("any-stats", "bigquery-sink", this._stats);
        });

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
                this._stats.batchRuns++;
                if(error){
                    this._stats.insertErrors++;
                    return reject(JSON.stringify(error));
                }

                this._stats.rowsInserted += rows.length;
                resolve();
            }));
    }

    beforeFirstPut(record) {

        // valueSchema contains the table description and must be cloned,
        // otherwise the reference would cause serious anomalies
        const tableDescription = JSON.parse(JSON.stringify(record.valueSchema));

        return new Promise((resolve, reject) => this.table.create(
            tableDescription,
            (error, table, apiResponse) => {
                if(error) {
                    // This might happen if multiple instances of the connector try to
                    // create the table concurrently. This is a simple remedy in case
                    // there is no external locking functionality available. The error
                    // is simply swallowed.
                    if (error.code === 409 && error.message.startsWith("Already Exists:")) {
                        return resolve();
                    }

                    return reject(error);
                }

                this._stats.tableCreated = true;
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
                const row = {
                    json: record.value
                }
                if(this.idColumn) {
                    row.insertId = record.value[this.idColumn].toString();
                }
                this.buffer.push(row);
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
                return callback(new Error(`Failed to check if dataset exists: ${JSON.stringify(error)}`));
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
                return callback(new Error(`Failed to check if table exists: ${JSON.stringify(error)}`));
            }

            this.initializedTable = exists;
            callback();
        });
    }
}

module.exports = BigQuerySinkTask;
