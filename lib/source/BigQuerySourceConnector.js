"use strict";

const { SourceConnector } = require("kafka-connect");
const BigQuery = require("@google-cloud/bigquery");

class BigQuerySourceConnector extends SourceConnector {

    start(properties, callback) {

        this.properties = properties;

        this.bigQuery = new BigQuery({this.properties.projectId});
        this.dataset = this.bigQuery.dataset(this.properties.dataset);
        this.table = this.dataset.table(this.properties.table);
        this.tableSchema = {};

        // TODO: check existence of project, dataset, table
        // TODO: get table schema via table.get
        // TODO: support authentication

        callback();
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
}

module.exports = BigQuerySourceConnector;
