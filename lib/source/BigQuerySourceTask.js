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
            tableDescription,
            idColumn
        } = this.properties;

        this.bigQuery = bigQuery;
        this.dataset = dataset;
        this.table = table;
        this.maxTasks = maxTasks;
        this.maxPollCount = maxPollCount;
        this.tableDescription = tableDescription;
        this.idColumn = idColumn;

        this.nextQuery = null; // use autoPaginate

        callback(null);
    }

    poll(callback) {

        const options = {
            autoPaginate: true,
            maxResults: this.maxPollCount
        };

        if (this.nextQuery) {
            options.pageToken = this.nextQuery.pageToken;
        }

        this.table.getRows(
            options,
            (error, rows, nextQuery, apiResponse) => {
                if (error) {
                    return callback(error);
                }

                this.nextQuery = nextQuery;

                const records = rows.map(row => {

                    const record = new SourceRecord();

                    record.key = row[this.idColumn];
                    record.keySchema = null;

                    if (!record.key) {
                        throw new Error("db results are missing row number");
                    }

                    record.value = row;
                    record.valueSchema = this.tableDescription;

                    record.timestamp = new Date().toISOString();
                    record.partition = -1;
                    record.topic = this.table.id;

                    this.parentConfig.emit("record-read", record.key.toString());
                    return record;
                });

                callback(null, records);
            }
        );
    }

    stop() {
        //empty (con is closed by connector)
    }
}

module.exports = BigQuerySourceTask;
