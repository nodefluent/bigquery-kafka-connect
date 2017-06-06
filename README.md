# bigquery-kafka-connect
Kafka Connect connector for Google BigQuery

[![Build Status](https://travis-ci.org/nodefluent/bigquery-kafka-connect.svg?branch=master)](https://travis-ci.org/nodefluent/bigquery-kafka-connect)

[![Coverage Status](https://coveralls.io/repos/github/nodefluent/bigquery-kafka-connect/badge.svg?branch=master)](https://coveralls.io/github/nodefluent/bigquery-kafka-connect?branch=master)

## Use API

```
npm install --save bigquery-kafka-connect
```

### bigquery -> kafka

```es6
const { runSourceConnector } = require("bigquery-kafka-connect");
runSourceConnector(config, [], onError).then(config => {
    //runs forever until: config.stop();
});
```

### kafka -> bigquery

```es6
const { runSinkConnector } = require("bigquery-kafka-connect");
runSinkConnector(config, [], onError).then(config => {
    //runs forever until: config.stop();
});
```

### kafka -> bigquery (with custom topic (no source-task topic))

```es6
const { runSinkConnector, ConverterFactory } = require("bigquery-kafka-connect");

const bigQuerySchema = {
    "fields": [
        { name: "id", type: "INTEGER", mode: "REQUIRED" },
        { name: "name", type: "STRING", mode: "REQUIRED" },
        { name: "info", type: "STRING", mode: "NULLABLE" }
    ]
};

const etlFunc = (messageValue, callback) => {

    //type is an example json format field
    if (messageValue.type === "publish") {
        return callback(null, {
            id: messageValue.payload.id,
            name: messageValue.payload.name,
            info: messageValue.payload.info
        });
    }

    if (messageValue.type === "unpublish") {
        return callback(null, null); //null value will cause deletion
    }

    callback(new Error("unknown messageValue.type"));
};

const converter = ConverterFactory.createSinkSchemaConverter(bigQuerySchema, etlFunc);

runSinkConnector(config, [converter], onError).then(config => {
    //runs forever until: config.stop();
});

/*
    this example would be able to store kafka message values
    that look like this (so completely unrelated to messages created by a default SourceTask)
    {
        payload: {
            id: 1,
            name: "first item",
            info: "some info"
        },
        type: "publish"
    }
*/
```

## Use CLI
note: in BETA :seedling:

```
npm install -g bigquery-kafka-connect
```

```
# run source etl: bigquery -> kafka
nkc-bigquery-source --help
```

```
# run sink etl: kafka -> bigquery
nkc-bigquery-sink --help
```

## Config(uration)
```es6
const config = {
    kafka: {
        zkConStr: "localhost:2181/",
        logger: null,
        groupId: "kc-bigquery-test",
        clientName: "kc-bigquery-test-name",
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
            requireAcks: 1,
            //ackTimeoutMs: 100,
            //partitionerType: 3
        }
    },
    topic: "sc_test_topic",
    partitions: 1,
    maxTasks: 1,
    pollInterval: 2000,
    produceKeyed: true,
    produceCompressionType: 0,
    connector: {
        batchSize: 500,
        maxPollCount: 500,
        projectId: "bq-project-id",
        dataset: "bq_dataset",
        table: "bq_table",
        idColumn: "id"
    },
    http: {
        port: 3149,
        middlewares: []
    },
    enableMetrics: true
};
```
