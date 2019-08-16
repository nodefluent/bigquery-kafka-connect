"use strict";

const async = require("async");

const { SourceConnector } = require("kafka-connect");
const { BigQuery } = require("@google-cloud/bigquery");

class BigQuerySourceConnector extends SourceConnector {

    start(properties, callback) {

        // TODO: support authentication options

        this.properties = properties;

        this.bigQuery = new BigQuery({projectId: this.properties.projectId});
        this.dataset = this.bigQuery.dataset(this.properties.dataset);
        this.table = this.dataset.table(this.properties.table);

        async.series(
            [
                done => this._checkDatasetExists(done),
                done => this._checkTableExists(done),
                done => this._getTableDescription(done)
            ],
            callback
        )
    }

    taskConfigs(maxTasks, callback) {

        const taskConfig = {
            maxTasks,
            maxPollCount: this.properties.maxPollCount,
            bigQuery: this.bigQuery,
            dataset: this.dataset,
            table: this.table,
            tableDescription: this.tableDescription,
            idColumn: this.properties.idColumn
        };

        callback(null, taskConfig);
    }

    stop() {
        //bigQuery closes itself after .run() finishes
    }

    _checkDatasetExists(callback) {
        this.dataset.exists((error, exists) => {
            if(error) {
                return callback(new Error(`Failed to check if dataset exists: ${JSON.stringify(error)}`));
            }

            if(!exists) {
                return callback(new Error("The specified dataset doesn't exist."));
            }

            callback();
        });
    }

    _checkTableExists(callback) {
        this.table.exists((error, exists) => {
            if(error) {
                return callback(JSON.stringify(error));
            }

            if(!exists) {
                return callback(new Error("The specified table doesn't exist."));
            }

            callback();
        });
    }

    _getTableDescription(callback) {
        this.table.get((error, tableDescription, apiResponse) => {
            if(error) {
                return callback(new Error(`Failed to get table description: ${JSON.stringify(error)}`));
            }

            const {schema, timePartitioning} = tableDescription;
            this.tableDescription = {schema, timePartitioning};

            callback();
        });
    }
}

module.exports = BigQuerySourceConnector;
