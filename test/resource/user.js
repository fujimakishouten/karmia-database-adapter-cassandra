/* vim: set expandtab tabstop=4 shiftwidth=4 softtabstop=4: */
/* eslint-env es6, mocha, node */
/* eslint-extends: eslint:recommended */
'use strict';


/**
 * Get birthday
 *
 * @param   {number} month
 * @param   {number} date
 * @param   {number} age
 * @returns {string}
 */
function birthday(month, date, age) {
    const today = new Date(),
        year = (4 > month) ? today.getFullYear() + 1 : today.getFullYear(),
        birth_year = year - age,
        birth_month = `00${month}`.slice(-2),
        birth_date = `00${date}`.slice(-2);

    return `${birth_year}-${birth_month}-${birth_date}`;
}


// Export module
module.exports = [
    {
        user_id: 1,
        email: 'honoka@μs.jp',
        name: 'Honoka Kosaka',
        birthday: birthday(8, 3, 16),
        blood_type: 'O',
        size: {
            height: 157,
            bust: 78,
            waist: 58,
            hip: 82
        },
        favorite_food: 'Strawberry',
        dislikes_food: 'Pepper',
        color: 'Orange',
        unit: 'Printemps'
    }, {
        user_id: 2,
        email: 'kotori@μs.jp',
        name: 'Kotori Minami',
        birthday: birthday(9, 12, 16),
        blood_type: 'O',
        size: {
            height: 159,
            bust: 80,
            waist: 58,
            hip: 80
        },
        favorite_food: 'Cheesecake',
        dislikes_food: 'Garlic',
        color: 'Gray',
        unit: 'Printemps'
    }, {
        user_id: 3,
        email: 'umi@μs.jp',
        name: 'Umi Sonoda',
        birthday: birthday(3, 15, 16),
        blood_type: 'A',
        size: {
            height: 159,
            bust: 76,
            waist: 58,
            hip: 80
        },
        favorite_food: 'Honoka',
        dislikes_food: 'Soda',
        color: 'Blue',
        unit: 'lily white'
    }, {
        user_id: 4,
        email: 'maki@μs.jp',
        name: 'Maki Nishikino',
        birthday: birthday(4, 19, 15),
        blood_type: 'AB',
        size: {
            height: 161,
            bust: 78,
            waist: 56,
            hip: 83
        },
        favorite_food: 'Tomato',
        dislikes_food: 'Orange',
        color: 'Red',
        unit: 'BiBi'
    }, {
        user_id: 5,
        email: 'rin@μs.jp',
        name: 'Rin Hoshizora',
        birthday: birthday(11, 1, 15),
        blood_type: 'A',
        size: {
            height: 155,
            bust: 75,
            waist: 59,
            hip: 80
        },
        favorite_food: 'Ramen',
        dislikes_food: 'Fish',
        color: 'Yellow',
        unit: 'lily white'
    }, {
        user_id: 6,
        email: 'hanayo@μs.jp',
        name: 'Hanayo Koizumi',
        birthday: birthday(1, 17, 15),
        blood_type: 'B',
        size: {
            height: 156,
            bust: 82,
            waist: 60,
            hip: 83
        },
        favorite_food: 'Rice',
        dislikes_food: '',
        color: 'Green',
        unit: 'Printemps'
    }, {
        user_id: 7,
        email: 'nico@μs.jp',
        name: 'Nico Yazawa',
        birthday: birthday(7, 22, 17),
        blood_type: 'A',
        size: {
            height: 154,
            bust: 74,
            waist: 57,
            hip: 72
        },
        favorite_food: 'Sweet treat',
        dislikes_food: 'Deviled food',
        color: 'Pink',
        unit: 'BiBi'
    }, {
        user_id: 8,
        email: 'nico@μs.jp',
        name: 'Nozomi Tojo',
        birthday: birthday(6, 9, 17),
        blood_type: 'O',
        size: {
            height: 159,
            bust: 90,
            waist: 60,
            hip: 82
        },
        favorite_food: 'Grilled meat',
        dislikes_food: 'Candy',
        color: 'Purple',
        unit: 'lily white'
    }, {
        user_id: 9,
        email: 'eri@μs.jp',
        name: 'Eri Ayase',
        birthday: birthday(10, 21, 17),
        blood_type: 'B',
        size: {
            height: 162,
            bust: 88,
            waist: 60,
            hip: 84
        },
        favorite_food: 'Chocolate',
        dislikes_food: 'Pickled Japanese apricot / Laver',
        color: 'Aqua',
        unit: 'BiBi'
    }
];



/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * c-hanging-comment-ender-p: nil
 * End:
 */
