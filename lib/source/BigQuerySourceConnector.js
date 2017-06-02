"use strict";

const { SourceConnector } = require("kafka-connect");
const BigQuery = require("@google-cloud/bigquery");

class BigQuerySourceConnector extends SourceConnector {

    start(properties, callback) {

        // TODO: support authentication options

        this.properties = properties;

        this.bigQuery = new BigQuery({this.properties.projectId});
        this.dataset = this.bigQuery.dataset(this.properties.dataset);
        this.table = this.dataset.table(this.properties.table);

        async.series(
            [
                done => this._checkDatasetExists(done),
                done => this._checkTableExists(done),
                done => this._getTableSchema(done)
            ]
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
            tableSchema: this.tableSchema
        };

        callback(null, taskConfig);
    }

    stop() {
        //bigQuery closes itself after .run() finishes
    }

    _checkDatasetExists(callback) {
        this.dataset.exists((error, exists) => {
            if(error) {
                return callback(JSON.stringfy(error));
            }

            if(!exists) {
                return callback("The specified dataset doesn't exist.");
            }

            callback();
        });
    }

    _checkTableExists(callback) {
        this.table.exists((error, exists) => {
            if(error) {
                return callback(JSON.stringfy(error));
            }

            if(!exists) {
                return callback("The specified table doesn't exist.");
            }

            callback();
        });
    }

    _getTableSchema(callback) {
        this.table.get((error, tableDescription, apiResponse) => {
            if(error) {
                return callback(JSON.stringify(error));
            }

            this.tableSchema = tableDescription.metadata.schema;

            callback();
        });
    }
}

module.exports = BigQuerySourceConnector;
