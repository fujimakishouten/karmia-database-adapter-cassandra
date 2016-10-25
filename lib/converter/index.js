/* vim: set expandtab tabstop=4 shiftwidth=4 softtabstop=4: */
/* eslint-env es6, mocha, node */
/* eslint-extends: eslint:recommended */
'use strict';



// Variables
const fs = require('fs'),
    path = require('path'),
    modules = {};


// Require modules
fs.readdirSync(__dirname).forEach(function (filename) {
    if ('index.js' === filename) {
        return;
    }

    const extension = path.extname(filename),
        name = filename.replace(extension, '');

    modules[name] = require(path.join(__dirname, name));
});


// Export modules
module.exports = function (options) {
    options = options || {};

    return Object.keys(modules).reduce(function (result, key) {
        result[key] = modules[key](options[key] || options);

        return result;
    }, {});
};



/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * c-hanging-comment-ender-p: nil
 * End:
 */
