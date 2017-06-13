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
     * Pass in the table description of the BigQuery Table
     * and a function that receives the message value
     * calls a callback(null, {}) with the transformed value
     * in form of the table that is defined by the BigQuery schema
     * returns an instance of a Converter that can be passed into the
     * Converter-Array param of the SinkConfig
     * @param {*} bigQueryTableDescription
     * @param {*} etlFunction
     * @return {}
     */
    static createSinkSchemaConverter(bigQueryTableDescription, etlFunction) {

        if (typeof bigQueryTableDescription !== "object") {
            throw new Error("bigQueryTableDescription must be an object.");
        }

        if (typeof etlFunction !== "function") {
            throw new Error("etlFunction must be a function.");
        }

        return new InjectableConverter(ConverterFactory._getSinkSchemaETL(bigQueryTableDescription, etlFunction));
    }

    static _getSinkSchemaETL(bigQueryTableDescription, etlFunction) {

        const schema = bigQueryTableDescription;
        const etl = etlFunction;

        return (message, callback) => {

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
