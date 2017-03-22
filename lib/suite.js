/* vim: set expandtab tabstop=4 shiftwidth=4 softtabstop=4: */
/* eslint-env es6, mocha, node */
/* eslint-extends: eslint:recommended */
'use strict';



// Variables
const karmia_utility_string = require('karmia-utility-string'),
    string = karmia_utility_string();


/**
 * KarmiaDatabaseAdapterCassandraTableSuite
 *
 * @class
 */
class KarmiaDatabaseAdapterCassandraTableSuite {
    /**
     * Constructor
     *
     * @param {Object} adapter
     * @param {string} name
     * @param {Array} tables
     * @param {*} id
     * @constructs KarmiaDatabaseAdapterCassandraTableSuite
     */
    constructor(adapter, name, tables, id) {
        const self = this,
            regexp = new RegExp(`^${string.camelCase(name)}`);
        self.adapter = adapter;
        self.name = name;
        self.tables = tables;
        self.id = id;

        tables.forEach(function (table_name) {
            const table = self.adapter.table(table_name),
                name = table_name.replace(regexp, '');
            self[string.camelCase(`get_${name}`)] = self.getter(table, self.id);
            self[string.camelCase(`set_${name}`)] = self.setter(table, self.id);
            self[string.camelCase(`remove_${name}`)] = self.deleter(table, self.id);
        });
    }

    /**
     * Get Id
     *
     * @returns {*}
     */
    getId() {
        const self = this;

        return self.id;
    }

    /**
     * Generate getter function
     *
     * @param   {Object} table
     * @param   {*} id
     * @returns {Function}
     */
    getter(table, id) {
        return function () {
            const parameters = [id].concat(Array.from(arguments)),
                conditions = table.key.reduce(function (collection, key, index) {
                    const value = parameters[index];
                    if (value) {
                        collection[key] = {$in: Array.isArray(value) ? value : [value]};
                    }

                    return collection;
                }, {});
            conditions[table.key[0]] = id;

            const args = [conditions].concat(parameters.slice(table.key.length));
            if (1 < table.key.length) {
                return table.find.apply(table, args);
            }

            return table.get.apply(table, args);
        };
    }

    /**
     * Generate setter function
     *
     * @param   {Object} table
     * @param   {*} id
     * @returns {Function}
     */
    setter(table, id) {
        return function () {
            const parameters = [id].concat(Array.from(arguments)),
                data = parameters[table.key.length] || {};
            table.key.forEach(function (key, index) {
                data[key] = parameters[index];
            });

            return table.set.apply(table, [data].concat(parameters.slice(table.key.length + 1)));
        };
    }

    /**
     * Generate deleter function
     *
     * @param   {Object} table
     * @param   {*} id
     * @returns {Function}
     */
    deleter(table, id) {
        return function () {
            const parameters = [id].concat(Array.from(arguments)),
                conditions = table.key.reduce(function (collection, key, index) {
                    const value = parameters[index];
                    if (value) {
                        collection[key] = {$in: Array.isArray(value) ? value : [value]};
                    }

                    return collection;
                }, {});
            conditions[table.key[0]] = id;

            return table.remove.apply(table, [conditions].concat(parameters.slice(table.key.length)));
        };
    }
}


// Export module
module.exports = function (adapter, name, tables, id) {
    return new KarmiaDatabaseAdapterCassandraTableSuite(adapter, name, tables, id);
};



/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * c-hanging-comment-ender-p: nil
 * End:
 */
