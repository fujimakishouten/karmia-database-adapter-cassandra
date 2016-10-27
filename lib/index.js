/* vim: set expandtab tabstop=4 shiftwidth=4 softtabstop=4: */
/* eslint-env es6, mocha, node */
/* eslint-extends: eslint:recommended */
"use strict";



// Variables
const express_cassandra = require('express-cassandra'),
    converter = require('./converter'),
    sequence = require('./sequence'),
    suite = require('./suite'),
    table = require('./table');


/**
 * KarmiaDatabaseAdapterCassandra
 *
 * @class
 */
class KarmiaDatabaseAdapterCassandra {
    /**
     * Constructor
     *
     * @param {Object} options
     * @constructs KarmiaDatabaseAdapterCassandra
     */
    constructor(options) {
        const self = this;
        self.config = options || {};

        self.converters = converter(self.config.converter || {});

        self.host = self.config.host || 'localhost';
        self.port = self.config.port || 9042;
        self.database = self.config.database || self.config.keyspace;
        self.options = self.config.options || {};
        self.user = self.config.user || self.config.username || self.options.user || self.options.username;
        self.password = self.config.password || self.config.pass || self.options.password || self.options.pass;
    }

    /**
     * Get connection
     *
     * @returns {Object}
     */
    getConnection() {
        const self = this;

        return self.connection;
    }

    /**
     * Connect to database
     *
     * @param   {Function} callback
     */
    connect(callback) {
        const self = this;
        if (self.connection) {
            return (callback) ? callback() : Promise.resolve();
        }

        const client_options = Object.assign({}, self.options.clientOptions || self.options.client || self.options),
            protocol_options = {port: self.port},
            query_options = {consistency: express_cassandra.consistencies.quorum},
            orm_options = Object.assign({}, self.options.ormOptions || self.options.orm || self.options),
            default_replication_strategy = {
                class: 'SimpleStrategy',
                replication_factor: 1
            };

        client_options.contactPoints = Array.isArray(self.host) ? self.host : [self.host];
        client_options.protocolOptions = Object.assign(protocol_options, client_options.protocolOptions || {});
        client_options.keyspace = self.database;
        client_options.queryOptions = Object.assign(query_options, client_options.queryOptions || {});

        orm_options.defaultReplicationStrategy = Object.assign(default_replication_strategy, orm_options.defaultReplicationStrategy || {});
        orm_options.migration = orm_options.migration || 'safe';
        orm_options.createKeyspace = ('createKeyspace' in orm_options) ? orm_options.createKeyspace : true;

        self.connection = new express_cassandra({
            clientOptions: client_options,
            ormOptions: orm_options
        });

        if (callback) {
            return self.connection.connect(callback);
        }

        return new Promise(function (resolve, reject) {
            self.connection.connect(function (error) {
                return (error) ? reject(error) : resolve();
            });
        });
    }

    /**
     * Disconnect from database
     *
     * @param {Function} callback
     */
    disconnect(callback) {
        const self = this;
        if (self.connection) {
            return new Promise(function (resolve, reject) {
                self.connection.close(function (error, result) {
                    return (error) ? reject(error) : resolve(result);
                });
            }).then(function () {
                return (callback) ? callback() : Promise.resolve();
            }).catch(function (error) {
                return (callback) ? callback(error) : Promise.reject(error);
            });
        }

        return (callback) ? callback() : Promise.resolve();
    }

    /**
     * Define schemas
     *
     * @param   {string} name
     * @param   {Object} schema
     * @returns {Object}
     */
    define(name, schema) {
        const self = this;
        self.schemas = self.schemas || {};
        self.schemas[name] = schema;

        return self;
    }

    /**
     * Configure
     *
     * @param callback
     */
    sync(callback) {
        const self = this;
        self.tables = self.tables || {};

        return (self.connection ? Promise.resolve() : self.connect()).then(function () {
            const parallels = Object.keys(self.schemas).map(function (key) {
                if (self.tables[key]) {
                    return;
                }

                const definition = self.converters.schema.convert(self.schemas[key]),
                    validation = self.converters.validator.convert(self.schemas[key]),
                    options = definition.options || {},
                    timestamps = {
                        createdAt: 'created_at',
                        updatedAt: 'updated_at'
                    };
                options.timestamps = ('timestamps' in options) ? options.timestamps : true;
                if (options.timestamps) {
                    options.timestamps = (options.timestamps instanceof Object) ? options.timestamps : {};
                    if ('createdAt' in options.timestamps) {
                        timestamps.createdAt = options.timestamps.createdAt;
                    }
                    if ('updatedAt' in options.timestamps) {
                        timestamps.updatedAt = options.timestamps.updatedAt;
                    }

                    const created_at = timestamps.createdAt,
                        updated_at = timestamps.updatedAt;
                    definition.fields[created_at] = definition.fields[created_at] || {type: 'timestamp'};
                    definition.fields[updated_at] = definition.fields[updated_at] || {type: 'timestamp'};
                }

                return new Promise(function (resolve, reject) {
                    const model = self.connection.loadSchema(key, definition, function (error) {
                        if (error) {
                            return reject(error);
                        }

                        self.tables[key] = table(self.connection, model, validation);
                        self.tables[key].timestamps = timestamps;

                        return resolve();
                    });
                });
            });

            return Promise.all(parallels);
        }).then(function (result) {
            return (callback) ? callback(null, result) : Promise.resolve(result);
        }).catch(function (error) {
            return (callback) ? callback(error) : Promise.reject(error);
        });
    }

    /**
     * Get table
     *
     * @param   {string} name
     * @returns {Object}
     */
    table(name) {
        const self = this;
        self.tables = self.tables || {};

        return self.tables[name];
    }

    /**
     * Get sequence
     *
     * @param   {string} key
     * @param   {Object} options
     * @returns {Object}
     */
    sequence(key, options) {
        const self = this;
        self.sequence = self.sequence || {};
        self.sequence[key] = self.sequence[key] || sequence(self.connection, key, options);

        return self.sequence[key];
    }

    /**
     * Get table suite
     *
     * @param   {string} name
     * @param   {Array} tables
     * @param   {number|string} id
     * @returns {Object}
     */
    suite(name, tables, id) {
        const self = this;

        return suite(self, name, tables, id);
    }
}


// Export module
module.exports = function (options) {
    return new KarmiaDatabaseAdapterCassandra(options || {});
};



/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * c-hanging-comment-ender-p: nil
 * End:
 */
