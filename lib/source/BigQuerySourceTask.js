"use strict";

const { SourceTask, SourceRecord } = require("kafka-connect");

const DEFAULT_ID_COLUMN = 'id';
const DEFAULT_UPDATED_AT_COLUMN = 'updated_at';

class BigQuerySourceTask extends SourceTask {

    _getQuery() {
        return [
            "SELECT * FROM",
            this.table.id,
            "WHERE",
            `${this.table.id}.${this.timestampColumn} = cast(@timestamp as TIMESTAMP)`,
            "AND",
            `${this.table.id}.${this.incrementingColumn} > @lastid OR ${this.table.id}.${this.timestampColumn} > cast(@timestamp as TIMESTAMP)`,
            "ORDER BY",
            `${this.table.id}.${this.timestampColumn}, ${this.table.id}.${this.incrementingColumn}`,
            "LIMIT @limit",
            "OFFSET @offset",
        ];
    }

    start(properties, callback, parentConfig) {
        this.parentConfig = parentConfig;
        this.properties = properties;

        const {
            bigQuery,
            dataset,
            table,
            maxTasks,
            maxPollCount,
            tableDescription,
            incrementingColumnName,
            timestampColumnName,
            initTimestamp,
        } = this.properties;

        this.bigQuery = bigQuery;
        this.dataset = dataset;
        this.table = table;
        this.incrementingColumn = incrementingColumnName || DEFAULT_ID_COLUMN;
        this.timestampColumn = timestampColumnName || DEFAULT_UPDATED_AT_COLUMN;
        this.maxTasks = maxTasks;
        this.maxPollCount = maxPollCount;
        this.tableDescription = tableDescription;
        this.currentOffset = 0;
        this.lastId = 0;
        this.lastTimestamp = initTimestamp || 0;

        this.parentConfig.on("get-stats", () => {
            this.parentConfig.emit("any-stats", "bigquery-source", this._stats);
        });

        callback(null);
    }

    poll(callback) {
        const options = {
            query: this._getQuery().join(" "),
            params: {
                limit: this.maxPollCount,
                offset: this.currentOffset,
                lastid: this.lastId,
                timestamp: this.lastTimestamp,
            }
        };


        this.table.query(options).then((results) => {
            this.currentOffset += results.length;

            let records = [];
            if (results.length !== 0) {
                records = results.map(result => {
                    const resultObject = result[0];
                    const record = new SourceRecord();
                    record.key = resultObject[this.incrementingColumn];
                    record.keySchema = null;

                    if (!record.key) {
                        throw new Error("Db results is missing increment");
                    }

                    record.value = resultObject;
                    record.valueSchema = this.tableDescription;

                    record.timestamp = new Date().toISOString();
                    record.partition = -1;
                    record.topic = this.table.id;

                    this.parentConfig.emit("record-read", record.key.toString());
                    this.lastId = resultObject[this.incrementingColumn];
                    this.lastTimestamp = resultObject[this.timestampColumn];

                    return record;
                });
            }

            callback(null, records);
        }).catch((err) => {
            callback(err);
        });
    }

    stop() {
        //empty (con is closed by connector)
    }
}

module.exports = BigQuerySourceTask;
