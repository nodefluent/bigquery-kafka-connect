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

        this.nextQuery = null; // use autoPaginate
        // table.get().then(data => {
        // data[0].metadata.numRows
        // data[0].metadata.schema
        // })

        callback(null);
    }

    poll(callback) {

        const options = {
            autoPaginate: true,
            maxResults: this.maxPollCount
        };

        if (this.nextQuery) {
            options.pageToken = nextQuery.pageToken;
        }

        this.table.getRows(
            options,
            (error, rows, nextQuery, apiResponse) => {
                if (error) {
                    return callback(error);
                }

                this.nextquery = nextQuery;

                const records = rows.map(row => {

                    const record = new SourceRecord();

                    // TODO: define key column
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
