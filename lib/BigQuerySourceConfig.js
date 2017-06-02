"use strict";

const {SourceConfig} = require("kafka-connect");

class BigQuerySourceConfig extends SourceConfig {

    constructor(...args){ super(...args); }

    run(){
        return super.run();
    }
}

module.exports = BigQuerySourceConfig;
