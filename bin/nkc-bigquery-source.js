#!/usr/bin/env node

const program = require("commander");
const path = require("path");
const { runSourceConnector } = require("./../index.js");
const pjson = require("./../package.json");
const loadConfig = require("./../config/loadConfig.js");

program
    .version(pjson.version)
    .option("-c, --config [string]", "Path to Config (alternatively)")
    .option("-k, --kafka [string]", "Zookeeper Connection String")
    .option("-n, --name [string]", "Kafka Client Name")
    .option("-t, --topic [string]", "Kafka Topic to Produce to")
    .option("-a, --partitions [integer]", "Amount of Kafka Topic Partitions")
    .option("-p, --project_id [string]", "GCloud project id")
    .option("-d, --dataset [string]", "BigQuery dataset name")
    .option("-b, --table [string]", "BigQuery table name")
    .option("-i, --id_column [string]", "Id column for insert ids and message keys")
    .option("-v, --interval [integer]", "Table poll interval (ms)")
    .option("-o, --max_pollcount [integer]", "Max row count per poll action")
    .parse(process.argv);

const config = loadConfig(program.config);

if (program.kafka) {
    config.kafka.zkConStr = program.kafka;
}

if (program.name) {
    config.kafka.clientName = program.name;
}

if (program.topic) {
    config.topic = program.topic;
}

if (program.partitions) {
    config.partitions = program.partitions;
}

if (program.project_id) {
    config.connector.project_id = program.project_id;
}

if (program.dataset) {
    config.connector.dataset = program.dataset;
}

if (program.table) {
    config.connector.table = program.table;
}

if (program.id_column) {
    config.connector.id_column = program.id_column;
}

if (program.interval) {
    config.pollInterval = program.interval;
}

if (program.max_pollcount) {
    config.connector.maxPollCount = program.max_pollcount;
}

runSourceConnector(config, [], console.log.bind(console)).then(sink => {

    const exit = (isExit = false) => {
        sink.stop();
        if (!isExit) {
            process.exit();
        }
    };

    process.on("SIGINT", () => {
        exit(false);
    });

    process.on("exit", () => {
        exit(true);
    });
});
