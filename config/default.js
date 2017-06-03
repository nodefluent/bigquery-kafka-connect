"use strict";

const path = require("path");

const config = {
    kafka: {
        zkConStr: "localhost:2181/",
        logger: null,
        groupId: "kc-bigquery-group",
        clientName: "kc-bigquery-client",
        workerPerPartition: 1,
        options: {
            sessionTimeout: 8000,
            protocol: ["roundrobin"],
            fromOffset: "earliest", //latest
            fetchMaxBytes: 1024 * 100,
            fetchMinBytes: 1,
            fetchMaxWaitMs: 10,
            heartbeatInterval: 250,
            retryMinTimeout: 250,
            autoCommit: true,
            autoCommitIntervalMs: 1000,
            requireAcks: 1,
            //ackTimeoutMs: 100,
            //partitionerType: 3
        }
    },
    topic: "bq_table_topic",
    partitions: 1,
    maxTasks: 1,
    pollInterval: 250,
    produceKeyed: true,
    produceCompressionType: 0,
    connector: {
        batchSize: 500,
        maxPollCount: 500,
        projectId: "bq-project-id",
        dataset: "bq_dataset",
        table: "bq_table",
        idColumn: "id"
    }
};

module.exports = config;
