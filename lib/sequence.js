/* vim: set expandtab tabstop=4 shiftwidth=4 softtabstop=4: */
/* eslint-env es6, mocha, node */
/* eslint-extends: eslint:recommended */
'use strict';


// Variables
const util = require('util'),
    schema = require('../schema/sequence');


/**
 * KarmiaDatabaseAdapterCassandraSequence
 *
 * @class
 */
class KarmiaDatabaseAdapterCassandraSequence {
    /**
     * Constructor
     *
     * @param {Object} connection
     * @param {string} key
     * @param {Object} options
     * @constructs KarmiaDatabaseAdapterCassandraSequence
     */
    constructor(connection, key, options) {
        const self = this;
        self.connection = connection;
        self.key = key;
        self.config = options || {};

        self.name = self.config.name || 'sequence';
    }

    /**
     * Sync schema
     *
     * @param callback
     * @returns {*}
     */
    model(callback) {
        const self = this;
        self.table = self.table || self.connection.instance[self.name];
        if (self.table) {
            return (callback) ? callback(null, self.table) : Promise.resolve(self.table);
        }

        return new Promise(function (resolve, reject) {
            self.table = self.connection.loadSchema(self.name, schema, function (error) {
                return (error) ? reject(error) : resolve();
            });
        }).then(function () {
            return (callback) ? callback(null, self.table) : Promise.resolve(self.table);
        }).catch(function (error) {
            return (callback) ? callback(error) : Promise.reject(error);
        });
    }

    /**
     * Get sequence value
     *
     * @param {Object} options
     * @param {Function} callback
     */
    get(options, callback) {
        if (options instanceof Function) {
            callback = options;
            options = {};
        }

        const self = this,
            parameters = Object.assign({}, options || {});

        return self.model().then(function () {
            return new Promise(function (resolve, reject) {
                self.table.findOne({key: self.key}, function (error, result) {
                    return (error) ? reject(error) : resolve(result);
                });
            });
        }).then(function (result) {
            let query,
                values;
            const value = (result) ? result.value : 0;
            if (result) {
                query = util.format('UPDATE %s SET value = ? WHERE key = ? IF value = ?', self.name);
                values = [value + 1, self.key, value];
            } else {
                query = util.format('INSERT INTO %s(key, value) VALUES(?, ?) IF NOT EXISTS', self.name);
                values = [self.key, value + 1];
            }

            return new Promise(function (resolve, reject) {
                parameters.prepare = true;
                self.table.execute_query(query, values, parameters, function (error) {
                    return (error) ? reject(error) : resolve(value + 1);
                });
            });
        }).then(function (result) {
            return (callback) ? callback(null, result) : Promise.resolve(result);
        }).catch(function (error) {
            return (callback) ? callback(null, error) : Promise.reject(error);
        });
    }
}


// Export module
module.exports = function (connection, key, options) {
    return new KarmiaDatabaseAdapterCassandraSequence(connection, key, options || {});
};



/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * c-hanging-comment-ender-p: nil
 * End:
 */

