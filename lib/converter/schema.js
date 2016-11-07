/* vim: set expandtab tabstop=4 shiftwidth=4 softtabstop=4: */
/* eslint-env es6, mocha, node */
/* eslint-extends: eslint:recommended */
'use strict';



// Variables
const util = require('util'),
    map = {
        array: 'list',
        boolean: 'boolean',
        integer: 'int',
        number: 'double',
        object: 'map',
        string: 'varchar'
    };


/**
 * KarmiaDatabaseAdapterCassandraConverterSchema
 *
 * @class
 */
class KarmiaDatabaseAdapterCassandraConverterSchema {
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
                return Object.keys(schemas).reduce(function (result, key) {
                    const property = ('properties' === key) ? 'fields' : key;
                    if ('fields' === property) {
                        result.fields = self.properties(schemas);
                    }

                    if ('indexes' === property) {
                        result.indexes = self.indexes(schemas);
                    }

                    if ('key' === property) {
                        result.key = Array.isArray(schemas.key) ? schemas.key : [schemas.key];
                    }

                    result[property] = convert(result[property] || schemas[key]);

                    return result;
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
        /**
         * Get typedef
         *
         * @param {string} type
         * @param {Object} fields
         */
        function typedef(type, fields) {
            const key = Object.keys(fields)[0],
                field_type = map[fields[key].type] || fields[key].type;
            if ('map' === type) {
                return util.format('<varchar,%s>', field_type);
            }

            return util.format('<%s>', field_type);
        }

        const maps = ['map', 'list', 'set'],
            properties = schemas.properties || schemas.fields,
            result = Object.keys(properties).reduce(function (result, key) {
                result[key] = Object.assign({}, properties[key]);
                result[key].type = map[result[key].type] || result[key].type;
                if (-1 < maps.indexOf(result[key].type)) {
                    const fields = result[key].properties || result[key].fields || {};
                    result[key].typeDef = result[key].typeDef || typedef(result[key].type, fields);
                }

                return result;
            }, {});

        (schemas.required || []).forEach(function (key) {
            result[key].rule = result[key].rule || {};
            result[key].rule.required = true;
            result[key].rule.validator = function (value) {
                return !!value;
            };
        });

        return result;
    }

    /**
     * Convert indexes property
     *
     * @param   {Object} schemas
     * @returns {Array}
     */
    indexes(schemas) {
        return schemas.indexes.reduce(function (result, index) {
            if (Array.isArray(index)) {
                const fields = (Object.getPrototypeOf(index[0]) === Object.prototype) ? Object.keys(index[0]) : index;
                if (1 === fields.length) {
                    result.push(fields);
                }

                return result;
            }

            if (Object.getPrototypeOf(index) === Object.prototype) {
                let fields = (index.fields) ? index.fields : Object.keys(index);
                fields = (Object.getPrototypeOf(fields) === Object.prototype) ? Object.keys(fields) : fields;
                if (1 === fields.length) {
                    result.push(fields);
                }

                return result;
            }

            result.push(index);

            return result;
        }, []);
    }
}


// Export module
module.exports = function () {
    return new KarmiaDatabaseAdapterCassandraConverterSchema();
};



/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * c-hanging-comment-ender-p: nil
 * End:
 */
