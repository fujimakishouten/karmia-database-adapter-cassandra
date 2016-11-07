/* vim: set expandtab tabstop=4 shiftwidth=4 softtabstop=4: */
/* eslint-env es6, mocha, node */
/* eslint-extends: eslint:recommended */
'use strict';



// Variables
const map = {
    ascii: 'string',
    bigint: 'number',
    blob: 'string',
    boolean: 'boolean',
    counter: 'number',
    decimal: 'number',
    double: 'number',
    float: 'number',
    inet: 'string',
    int: 'number',
    list: 'array',
    map: 'object',
    set: 'array',
    text: 'string',
    timestamp: 'string',
    uuid: 'string',
    timeuuid: 'string',
    varchar: 'string',
    varint: 'number'
};



/**
 * KarmiaDatabaseAdapterCassandraConverterValidator
 *
 * @class
 */
class KarmiaDatabaseAdapterCassandraConverterValidator {
    /**
     * Convert schema
     *
     * @param   {Object} schemas
     * @returns {Object}
     */
    convert(schemas) {
        const self = this;

        return (function convert(schemas) {
            if (Array.isArray(schemas)) {
                return schemas.map(convert);
            }

            if (Object.getPrototypeOf(schemas) === Object.prototype) {
                return Object.keys(schemas).reduce(function (collection, key) {
                    const property = ('fields' === key) ? 'properties' : key;
                    if ('properties' === property) {
                        collection.properties = self.properties(schemas);
                        collection.required = self.required(schemas);
                    }

                    collection[property] = convert(collection[property] || schemas[key]);

                    return collection;
                }, {});
            }

            return schemas;
        })(schemas);
    }

    /**
     * Convert properties
     *
     * @param   {Object} schemas
     * @returns {Object}
     */
    properties(schemas) {
        const property = (schemas.fields) ? 'fields' : 'properties';

        return Object.keys(schemas[property]).reduce(function (collection, key) {
            collection[key] = Object.assign({}, schemas[property][key]);
            collection[key].type = map[collection[key].type] || collection[key].type;

            return collection;
        }, {});
    }

    /**
     * Convert required
     *
     * @param schemas
     * @returns {Array}
     */
    required(schemas) {
        const keys = schemas.key || [],
            property = (schemas.fields) ? 'fields' : 'properties',
            result = (Array.isArray(keys) ? keys : [keys]).concat(schemas.required || []);
        Object.keys(schemas[property]).forEach(function (key) {
            const rule = schemas[property][key].rule || {};
            if (rule.required) {
                result.push(key);
            }
        });

        return Array.from(new Set(result));
    }
}


// Export module
module.exports = function () {
    return new KarmiaDatabaseAdapterCassandraConverterValidator();
};



/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * c-hanging-comment-ender-p: nil
 * End:
 */
