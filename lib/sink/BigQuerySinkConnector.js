"use strict";

const { SinkConnector } = require("kafka-connect");
const BigQuery = require("@google-cloud/bigquery");

class BigQuerySinkConnector extends SinkConnector {

    start(properties, callback) {

        this.properties = properties;

        this.bigQuery = new BigQuery({projectId: this.properties.projectId});
        this.dataset = this.bigQuery.dataset(this.properties.dataset);
        this.table = this.dataset.table(this.properties.table);

        callback();
    }

    taskConfigs(maxTasks, callback) {

        const taskConfig = {
            maxTasks,
            batchSize: this.properties.batchSize,
            bigQuery: this.bigQuery,
            dataset: this.dataset,
            table: this.table
        };

        callback(null, taskConfig);
    }

    stop() {
        this.bigQuery.close();
    }
}

module.exports = BigQuerySinkConnector;
