/* vim: set expandtab tabstop=4 shiftwidth=4 softtabstop=4: */
/* eslint-env es6, mocha, node */
/* eslint-extends: eslint:recommended */
'use strict';



// Variables
const util = require('util'),
    tv4 = require('tv4'),
    tv4_formats = require('tv4-formats');
tv4.addFormat(tv4_formats);


/**
 * KarmiaDatabaseAdapterCassandraTable
 *
 * @class
 */
class KarmiaDatabaseAdapterCassandraTable {
    /**
     * Constructor
     *
     * @param {Object} connection
     * @param {Object} model
     * @param {Object} validation
     * @constructs KarmiaDatabaseAdapterCassandraTable
     */
    constructor(connection, model, validation) {
        const self = this;
        self.connection = connection;
        self.model = model;
        self.validation = validation;

        self.key = Array.isArray(validation.key) ? validation.key : [validation.key];
        self.fields = Object.keys(validation.properties);
        self.ttl = validation.ttl || 0;
    }

    /**
     * Validate data
     *
     * @param {Object} data
     * @param {Function} callback
     */
    validate(data, callback) {
        const self = this,
            result = tv4.validateMultiple(data, self.validation);
        if (result.errors.length) {
            return (callback) ? callback(result.errors) : Promise.reject(result.errors);
        }

        return (callback) ? callback(null, data) : Promise.resolve(data);
    }

    /**
     * Count items
     *
     * @param   {Object} conditions
     * @param   {Object} options
     * @param   {Function} callback
     */
    count(conditions, options, callback) {
        if (conditions instanceof Function) {
            callback = conditions;
            conditions = {};
            options = {};
        }

        if (options instanceof Function) {
            callback = options;
            options = {};
        }
        conditions = conditions || {};

        const self = this,
            keys = Object.keys(conditions),
            parameters = Object.keys(conditions).map(function (key) {
                return conditions[key];
            });
        let query = util.format('SELECT COUNT(*) FROM %s', self.model._properties.table_name);
        if (keys.length) {
            query = util.format('%s WHERE %s = ?', query, keys.join(' = ? AND '));
        }

        return new Promise(function (resolve, reject) {
            self.model.execute_query(query, parameters, options, function (error, result) {
                return (error) ? reject(error) : resolve(result.first().get('count').toNumber());
            });
        }).then(function (result) {
            return (callback) ? callback(null, result) : Promise.resolve(result);
        }).catch(function (error) {
            return (error) ? callback(error) : Promise.reject(error);
        });
    }

    /**
     * Find item
     *
     * @param   {Object} conditions
     * @param   {Object} projection
     * @param   {Object} options
     * @param   {Function} callback
     */
    get(conditions, projection, options, callback) {
        if (conditions instanceof Function) {
            callback = conditions;
            conditions = {};
            projection = {};
            options = {};
        }

        if (projection instanceof Function) {
            callback = projection;
            projection = {};
            options = {};
        }

        if (options instanceof Function) {
            callback = options;
            options = {};
        }

        conditions = conditions || {};
        projection = projection || {};
        options = options || {};

        const self = this,
            parameters = Object.assign({}, options || {}),
            attributes = self.getAttributes(projection);
        parameters.limit = parameters.limit || 1;
        if (attributes) {
            parameters.select = attributes;
        }

        if (!Object.keys(conditions).length) {
            return (callback) ? callback(null, null) : Promise.resolve(null);
        }

        return new Promise(function (resolve, reject) {
            self.model.findOne(conditions, parameters, function (error, result) {
                return (error) ? reject(error) : resolve(result || null);
            });
        }).then(function (result) {
            return (callback) ? callback(null, result) : Promise.resolve(result);
        }).catch(function (error) {
            return (callback) ? callback(error) : Promise.reject(error);
        });
    }

    /**
     * Find items
     *
     * @param   {Object} conditions
     * @param   {Object} projection
     * @param   {Object} options
     * @param   {Function} callback
     */
    find(conditions, projection, options, callback) {
        if (conditions instanceof Function) {
            callback = conditions;
            conditions = {};
            projection = {};
            options = {};
        }

        if (projection instanceof Function) {
            callback = projection;
            projection = {};
            options = {};
        }

        if (options instanceof Function) {
            callback = options;
            options = {};
        }

        conditions = conditions || {};
        projection = projection || {};
        options = options || {};

        const self = this,
            parameters = Object.assign({}, options || {}),
            attributes = self.getAttributes(projection);
        if (attributes) {
            parameters.select = attributes;
        }

        return new Promise(function (resolve, reject) {
            self.model.find(conditions, parameters, function (error, result) {
                return (error) ? reject(error) : resolve(result);
            });
        }).then(function (result) {
            return (callback) ? callback(null, result) : Promise.resolve(result);
        }).catch(function (error) {
            return (callback) ? callback(error) : Promise.reject(error);
        });
    }

    /**
     * Save item
     *
     * @param {Object} data
     * @param {Object} options
     * @param {Function} callback
     */
    set(data, options, callback) {
        if (options instanceof Function) {
            callback = options;
            options = {};
        }

        const self = this,
            keys = self.key.reduce(function (collection, key) {
                collection[key] = data[key];

                return collection;
            }, {}),
            values = self.fields.reduce(function (collection, key) {
                if (key in data) {
                    collection[key] = data[key];
                }

                return collection;
            }, {});

        return new Promise(function (resolve, reject) {
            self.model.findOne(keys, function (error, result) {
                return (error) ? reject(error) : resolve(result);
            });
        }).then(function (result) {
            const model = (result) ? Object.assign(result, values) : new self.model(values),
                parameters = Object.assign({}, options || {}),
                current_time = new Date();
            if (self.timestamps.createdAt) {
                model[self.timestamps.createdAt] = model[self.timestamps.createdAt] || current_time;
            }
            if (self.timestamps.updatedAt) {
                model[self.timestamps.updatedAt] = current_time;
            }
            if (self.ttl) {
                parameters.ttl = parameters.ttl || self.ttl;
            }

            return new Promise(function (resolve, reject) {
                model.save(parameters, function (error) {
                    return (error) ? reject(error) : resolve(model);
                });
            });
        }).then(function (result) {
            return (callback) ? callback(null, result) : Promise.resolve(result);
        }).catch(function (error) {
            return (callback) ? callback(error) : Promise.reject(error);
        });
    }

    /**
     * Remove items
     *
     * @param {Object} conditions
     * @param {Object} options
     * @param {Function} callback
     */
    remove(conditions, options, callback) {
        if (conditions instanceof Function) {
            callback = conditions;
            conditions = {};
        }

        if (options instanceof Function) {
            callback = options;
        }

        const self = this;
        conditions = conditions || {};

        if (!Object.keys(conditions).length) {
            return (callback) ? callback() : Promise.resolve();
        }

        return new Promise(function (resolve, reject) {
            self.model.delete(conditions, function (error, result) {
                return (error) ? reject(error) : resolve(result);
            });
        }).then(function (result) {
            return (callback) ? callback(null, result) : Promise.resolve(result);
        }).catch(function (error) {
            return (callback) ? callback(error) : Promise.reject(error);
        });
    }

    /**
     * Convert projection to attributes
     *
     * @param {Object} projection
     * @returns {Array|Object}
     */
    getAttributes(projection) {
        const parameters = projection || {};
        if (Object.getPrototypeOf(parameters) === Object.prototype) {
            return Object.keys(parameters).reduce(function (collection, key) {
                if (parameters[key]) {
                    collection = collection || [];
                    collection.push(key);
                }

                return collection;
            }, null);
        }

        return Array.isArray(parameters) ? parameters : [parameters];
    }
}


// Export module
module.exports = function (connection, model, schema) {
    return new KarmiaDatabaseAdapterCassandraTable(connection, model, schema);
};



/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * c-hanging-comment-ender-p: nil
 * End:
 */

