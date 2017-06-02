"use strict";

const { Converter } = require("kafka-connect");

class InjectableConverter extends Converter {

    constructor(etl) {
        super();
        this.etl = etl;
    }

    fromConnectData(data, callback) {
        callback(null, data); //no action required, as we produce objects directly
    }

    toConnectData(message, callback) {
        this.etl(message, callback);
    }
}

class ConverterFactory {

    /**
     * Pass in the JSON Schema of the BigQuery Table
     * and a function that receives the message value
     * calls a callback(null, {}) with the transformed value
     * in form of the table that is defined by the BigQuery schema
     * returns an instance of a Converter that can be passed into the
     * Converter-Array param of the SinkConfig
     * @param {*} bigQuerySchema
     * @param {*} etlFunction
     * @return {}
     */
    static createSinkSchemaConverter(bigQuerySchema, etlFunction) {

        if (typeof bigQuerySchema !== "object") {
            throw new Error("bigQuerySchema must be an object.");
        }

        if (typeof etlFunction !== "function") {
            throw new Error("etlFunction must be a function.");
        }

        return new InjectableConverter(ConverterFactory._getSinkSchemaETL(bigQuerySchema, etlFunction));
    }

    static _getSinkSchemaETL(bigQuerySchema, etlFunction) {

        const schema = bigQuerySchema;
        const etl = etlFunction;

        return function(message, callback) {

            etlFunction(message.value, (error, messageValue) => {

                if (error) {
                    return callback(error);
                }

                message.value = {
                    key: message.key,
                    keySchema: null,
                    value: messageValue,
                    valueSchema: Object.assign({}, schema),
                    partition: message.partition,
                    timestamp: new Date().toISOString(),
                    topic: message.topic
                };

                callback(null, message);
            });
        };
    }
}

module.exports = ConverterFactory;
