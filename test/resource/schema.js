/* vim: set expandtab tabstop=4 shiftwidth=4 softtabstop=4: */
/* eslint-env es6, mocha, node */
/* eslint-extends: eslint:recommended */
'use strict';



// Export module
module.exports = {
    title: "User",
    description: "User data schema",
    key: ['user_id'],
    required: ['user_id', 'email', 'name'],
    properties: {
        user_id: {
            type: 'integer'
        },
        email: {
            type: 'string'
        },
        name: {
            type: 'string'
        },

        birthday: {
            type: 'string',
            format: 'date'
        },
        blood_type: {
            type: 'string',
            enum: ['A', 'B', 'O', 'AB']
        },
        size: {
            type: 'object',
            properties: {
                height: {
                    type: 'integer'
                },
                bust: {
                    type: 'integer'
                },
                waist: {
                    type: 'integer'
                },
                hip: {
                    type: 'integer'
                }
            }
        },
        favorite_food: {
            type: 'string'
        },
        dislikes_food: {
            type: 'string'
        },
        color: {
            type: 'string'
        },
        unit: {
            type: 'string'
        }
    },
    indexes: ['email', 'unit']
};



/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * c-hanging-comment-ender-p: nil
 * End:
 */
