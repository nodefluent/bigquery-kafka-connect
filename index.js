"use strict";

const BigQuerySourceConfig = require("./lib/BigQuerySourceConfig.js");
const BigQuerySinkConfig = require("./lib/BigQuerySinkConfig.js");

const BigQuerySourceConnector = require("./lib/source/BigQuerySourceConnector.js");
const BigQuerySinkConnector = require("./lib/sink/BigQuerySinkConnector.js");

const BigQuerySourceTask = require("./lib/source/BigQuerySourceTask.js");
const BigQuerySinkTask = require("./lib/sink/BigQuerySinkTask.js");

const JsonConverter = require("./lib/utils/JsonConverter.js");
const ConverterFactory = require("./lib/utils/ConverterFactory.js");

const runSourceConnector = (properties, converters = [], onError = null) => {

    const config = new BigQuerySourceConfig(properties,
        BigQuerySourceConnector,
        BigQuerySourceTask, [JsonConverter].concat(converters));

    if (onError) {
        config.on("error", onError);
    }

    return config.run().then(() => {
        return config;
    });
};

const runSinkConnector = (properties, converters = [], onError = null) => {

    const config = new BigQuerySinkConfig(properties,
        BigQuerySinkConnector,
        BigQuerySinkTask, [JsonConverter].concat(converters));

    if (onError) {
        config.on("error", onError);
    }

    return config.run().then(() => {
        return config;
    });
};

module.exports = {
    runSourceConnector,
    runSinkConnector,
    ConverterFactory
};
