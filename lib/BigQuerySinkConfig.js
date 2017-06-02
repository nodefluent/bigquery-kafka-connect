"use strict";

const {SinkConfig} = require("kafka-connect");

class BigQuerySinkConfig extends SinkConfig {

    constructor(...args){ super(...args); }

    run(){
        return super.run();
    }
}

module.exports = BigQuerySinkConfig;
