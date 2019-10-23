"use strict";

const { SourceTask, SourceRecord } = require("kafka-connect");

const DEFAULT_ID_COLUMN = 'id';
const DEFAULT_UPDATED_AT_COLUMN = 'updated_at';

class BigQuerySourceTask extends SourceTask {

    _getQuery() {
        return [
            "SELECT",
            "*",
            "FROM",
            this.table.id,
            "WHERE",
            `${this.table.id}.${this.timestampColumn} = @timestamp`,
            "AND",
            `${this.table.id}.${this.incrementingColumn} > @lastid ) OR ${this.table.id}.${this.timestampColumn} > @timestamp)`,
            "ORDER BY",
            `${this.table.id}.${this.timestampColumn}, ${this.table.id}.${this.incrementingColumn}`,
            "LIMIT",
            "@limit",
            "OFFSET",
            "@offset"
        ];
    }

    start(properties, callback, parentConfig) {

        this.parentConfig = parentConfig;

        this.properties = properties;
        const {
            bigQuery,
            dataset,
            table,
            maxTasks,
            maxPollCount,
            tableDescription,
            incrementingColumnName,
            timestampColumnName,
            idColumn,
            initTimestamp,
        } = this.properties;

        this.bigQuery = bigQuery;
        this.dataset = dataset;
        this.table = table;
        this.incrementingColumn = incrementingColumnName || DEFAULT_ID_COLUMN;
        this.timestampColumn = timestampColumnName || DEFAULT_UPDATED_AT_COLUMN;
        this.maxTasks = maxTasks;
        this.maxPollCount = maxPollCount;
        this.tableDescription = tableDescription;
        this.idColumn = idColumn;
        this.lastId = 0;
        this.lastTimestamp = initTimestamp || 0;

        this.parentConfig.on("get-stats", () => {
            this.parentConfig.emit("any-stats", "bigquery-source", this._stats);
        });

        callback(null);
    }

    poll(callback) {
        const options = {
            params: {
                limit: this.maxPollCount,
                offset: 0,
                lastid: this.lastId,
                timestamp: this.lastTimestamp,
            }
        };

        const query = this._getQuery().join(" ");

        console.log(query);

        this.table.query(
            query,
            options,
            (err, rows) => {
                console.log(err, rows);
            }
        );
    }

    stop() {
        //empty (con is closed by connector)
    }
}

module.exports = BigQuerySourceTask;
