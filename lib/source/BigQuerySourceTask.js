"use strict";

const { SourceTask, SourceRecord } = require("kafka-connect");

class BigQuerySourceTask extends SourceTask {

    start(properties, callback, parentConfig) {

        this.parentConfig = parentConfig;

        this.properties = properties;
        const {
            bigQuery,
            dataset,
            table,
            maxTasks,
            maxPollCount,
            tableSchema
        } = this.properties;

        this.bigQuery = bigQuery;
        this.dataset = dataset;
        this.table = table;
        this.maxTasks = maxTasks;
        this.maxPollCount = maxPollCount;
        this.tableSchema = tableSchema;

        // TODO: get max row number
        this.currentOffset = 0; // use row number to query (alternative: autoPaginate?)

        callback(null);
    }

    poll(callback) {

        this.table.getRows(
            {
                autoPaginate: false,
                maxResults: this.maxPollCount
            },
            (error, rows, _nextQuery, apiResponse) => {
                if (error) {
                    return callback(error);
                }

                this.currentOffset += rows.length;

                const records = rows.map(row => {

                    const record = new SourceRecord();

                    record.key = row.number;
                    record.keySchema = null;

                    if (!record.key) {
                        throw new Error("db results are missing row number");
                    }

                    record.value = result;
                    record.valueSchema = this.tableSchema;

                    record.timestamp = new Date().toISOString();
                    record.partition = -1;
                    record.topic = this.table;

                    this.parentConfig.emit("record-read", record.key.toString());
                    return record;
                });
            }
        );
    }

    stop() {
        //empty (con is closed by connector)
    }
}

module.exports = BigQuerySourceTask;
