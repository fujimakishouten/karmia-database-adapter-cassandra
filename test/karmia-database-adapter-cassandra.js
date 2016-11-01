/* vim: set expandtab tabstop=4 shiftwidth=4 softtabstop=4: */
/* eslint-env es6, mocha, node */
/* eslint-extends: eslint:recommended */
'use strict';



// Variables
const expect = require('expect.js'),
    adapter = require('../'),
    schema = require('./resource/schema'),
    fixture = require('./resource/user'),
    options = {
        host: 'localhost',
        port: 9042,
        database: 'karmia_database_adapter_cassandra'
    };


// Test
describe('karmia-database-adapter-cassandra', function () {
    describe('getConnection', function () {
        it('Should not get connection', function (done) {
            const database = adapter(options);
            expect(database.getConnection()).to.be(undefined);

            done();
        });

        it('Should get connection', function (done) {
            const database = adapter(options);
            database.connect().then(function () {
                const connection = database.getConnection();
                expect(connection.constructor.name).to.be('f');

                done();
            });
        });

        it('Should get existing connection', function (done) {
            const connection = {name: 'TEST_CONNECTION'},
                database = adapter(options, connection);

            expect(database.getConnection()).to.be(connection);

            done();
        });
    });

    describe('connect', function () {
        describe('Should connect to database', function () {
            it('Promise', function (done) {
                const database = adapter(options);
                database.connect().then(function () {
                    const connection = database.getConnection();
                    expect(connection.constructor.name).to.be('f');

                    done();
                }).catch(function (error) {
                    done(error);
                });
            });

            it('Callback', function (done) {
                const database = adapter(options);
                database.connect(function (error) {
                    if (error) {
                        return done(error);
                    }

                    const connection = database.getConnection();
                    expect(connection.constructor.name).to.be('f');

                    done();
                });
            });
        });
    });

    describe('disconnect', function () {
        describe('Should disconnect from database', function () {
            describe('Connected', function () {
                it('Promise', function (done) {
                    const database = adapter(options);
                    database.connect().then(function () {
                        return database.disconnect();
                    }).then(function (result) {
                        expect(result).to.be(undefined);

                        done();
                    }).catch(done);
                });

                it('Callback', function (done) {
                    const database = adapter(options);
                    database.connect().then(function () {
                        database.disconnect(function (error, result) {
                            if (error) {
                                return done(error);
                            }

                            expect(result).to.be(undefined);

                            done();
                        });
                    });
                });
            });

            describe('Not connected', function () {
                it('Promise', function (done) {
                    const database = adapter(options);
                    database.disconnect().then(function (result) {
                        expect(result).to.be(undefined);

                        done();
                    }).catch(done);
                });

                it('Callback', function (done) {
                    const database = adapter(options);
                    database.disconnect(function (error, result) {
                        if (error) {
                            return done(error);
                        }

                        expect(result).to.be(undefined);

                        done();
                    });
                });
            });
        });
    });

    describe('define', function () {
        it('Should define schema', function (done) {
            const database = adapter(options),
                key = 'user',
                result = database.define(key, schema);

            expect(result).to.be(database);
            expect(result.schemas[key]).to.eql(schema);

            done();
        });
    });

    describe('sync', function () {
        describe('Should sync database', function () {
            it('Promise', function (done) {
                const database = adapter(options),
                    key = 'user';
                database.define(key, schema).sync().then(function () {
                    expect(database.tables[key].constructor.name).to.be('KarmiaDatabaseAdapterCassandraTable');

                    done();
                });
            });

            it('Callback', function (done) {
                const database = adapter(options),
                    key = 'user';
                database.define(key, schema).sync(function () {
                    expect(database.tables[key].constructor.name).to.be('KarmiaDatabaseAdapterCassandraTable');

                    done();
                }).catch(function (error) {
                    done(error);
                });
            });

            it('Call multitime', function (done) {
                const database = adapter(options),
                    key = 'user';
                database.define(key, schema).sync().then(function () {
                    return database.sync();
                }).then(function () {
                    expect(database.tables[key].constructor.name).to.be('KarmiaDatabaseAdapterCassandraTable');

                    done();
                });
            });
        });
    });

    describe('table', function () {
        const database = adapter(options),
            key = 'user';

        before(function (done) {
            database.define(key, schema).sync().then(function () {
                const table = database.table(key);

                return fixture.reduce(function (promise, data) {
                    return promise.then(function () {
                        return table.set(data);
                    });
                }, Promise.resolve());
            }).then(function () {
                done();
            }).catch(function (error) {
                done(error);
            });
        });

        it('Should get table', function (done) {
            const table = database.table(key);
            expect(table.constructor.name).to.be('KarmiaDatabaseAdapterCassandraTable');

            done();
        });

        describe('validation', function () {
            describe('Validation OK', function () {
                it('Promise', function (done) {
                    const table = database.table(key);

                    table.validate(fixture[0]).then(function (result) {
                        expect(result).to.be(fixture[0]);

                        done();
                    });
                });

                it('Callback', function (done) {
                    const table = database.table(key);

                    table.validate(fixture[0], function (error, result) {
                        if (error) {
                            return done(error);
                        }

                        expect(result).to.be(fixture[0]);

                        done();
                    });
                });
            });

            describe('Validation error', function () {
                it('Promise', function (done) {
                    const table = database.table(key);

                    table.validate({}).catch(function (error) {
                        expect(error).to.be.an('array');
                        expect(error).to.have.length(3);

                        done();
                    });
                });

                it('Callback', function (done) {
                    const table = database.table(key);

                    table.validate({}, function (error) {
                        expect(error).to.be.an('array');
                        expect(error).to.have.length(3);

                        done();
                    });
                });
            });
        });

        describe('count', function () {
            describe('Should count items', function () {
                it('Promise', function (done) {
                    const table = database.table(key);

                    table.count().then(function (result) {
                        expect(result).to.be(9);

                        done();
                    }).catch(function (error) {
                        done(error);
                    });
                });

                it('Callback', function (done) {
                    const table = database.table(key);

                    table.count(function (error, result) {
                        if (error) {
                            return done(error);
                        }

                        expect(result).to.be(9);

                        done();
                    });
                });
            });
        });

        describe('get', function () {
            describe('Should get item', function () {
                it('Promose', function (done) {
                    const table = database.table(key),
                        conditions = {user_id: 1};
                    table.get(conditions).then(function (result) {
                        const data = fixture[0];

                        expect(data['user_id']).to.be(result['user_id']);
                        expect(data['email']).to.be(result['email']);
                        expect(data['name']).to.be(result['name']);
                        expect(data['birthday']).to.be(result['birthday']);
                        expect(data['blood_type']).to.be(result['blood_type']);
                        expect(data['size']).to.eql(result['size']);
                        expect(data['favorite_food']).to.be(result['favorite_food']);
                        expect(data['dislikes_food']).to.be(result['dislikes_food']);
                        expect(data['color']).to.be(result['color']);
                        expect(data['unit']).to.be(result['unit']);

                        done();
                    }).catch(function (error) {
                        done(error);
                    });
                });

                it('Callback', function (done) {
                    const table = database.table(key),
                        conditions = {user_id: 1};
                    table.get(conditions, function (error, result) {
                        if (error) {
                            return done(error);
                        }

                        const data = fixture[0];

                        expect(data['user_id']).to.be(result['user_id']);
                        expect(data['email']).to.be(result['email']);
                        expect(data['name']).to.be(result['name']);
                        expect(data['birthday']).to.be(result['birthday']);
                        expect(data['blood_type']).to.be(result['blood_type']);
                        expect(data['size']).to.eql(result['size']);
                        expect(data['favorite_food']).to.be(result['favorite_food']);
                        expect(data['dislikes_food']).to.be(result['dislikes_food']);
                        expect(data['color']).to.be(result['color']);
                        expect(data['unit']).to.be(result['unit']);

                        done();
                    });
                });
            });

            describe('Should not get item', function () {
                it('Promise', function (done) {
                    const table = database.table(key);

                    table.get().then(function (result) {
                        expect(result).to.be(null);

                        done();
                    });
                });

                it('Callback', function (done) {
                    const table = database.table(key);

                    table.get(function (error, result) {
                        if (error) {
                            return done(error);
                        }

                        expect(result).to.be(null);

                        done();
                    });
                });
            });
        });

        describe('find', function () {
            describe('Should find all items', function () {
                it('Promise', function (done) {
                    const table = database.table(key);

                    table.find().then(function (result) {
                        result.sort(function (value1, value2) {
                            return value1.user_id - value2.user_id;
                        });

                        fixture.forEach(function (data, index) {
                            expect(data['user_id']).to.be(result[index]['user_id']);
                            expect(data['email']).to.be(result[index]['email']);
                            expect(data['name']).to.be(result[index]['name']);
                            expect(data['birthday']).to.be(result[index]['birthday']);
                            expect(data['blood_type']).to.be(result[index]['blood_type']);
                            expect(data['size']).to.eql(result[index]['size']);
                            expect(data['favorite_food']).to.be(result[index]['favorite_food']);
                            expect(data['dislikes_food']).to.be(result[index]['dislikes_food']);
                            expect(data['color']).to.be(result[index]['color']);
                            expect(data['unit']).to.be(result[index]['unit']);
                        });

                        done();
                    });
                });

                it('Callback', function (done) {
                    const table = database.table(key);

                    table.find(function (error, result) {
                        if (error) {
                            return done(error);
                        }

                        result.sort(function (value1, value2) {
                            return value1.user_id - value2.user_id;
                        });

                        fixture.forEach(function (data, index) {
                            expect(data['user_id']).to.be(result[index]['user_id']);
                            expect(data['email']).to.be(result[index]['email']);
                            expect(data['name']).to.be(result[index]['name']);
                            expect(data['birthday']).to.be(result[index]['birthday']);
                            expect(data['blood_type']).to.be(result[index]['blood_type']);
                            expect(data['size']).to.eql(result[index]['size']);
                            expect(data['favorite_food']).to.be(result[index]['favorite_food']);
                            expect(data['dislikes_food']).to.be(result[index]['dislikes_food']);
                            expect(data['color']).to.be(result[index]['color']);
                            expect(data['unit']).to.be(result[index]['unit']);
                        });

                        done();
                    });
                });
            });

            describe('Should find items', function () {
                it('Promise', function (done) {
                    const table = database.table(key),
                        conditions = {
                            user_id: {
                                $in: [1, 2, 3]
                            }
                        };
                    table.find(conditions).then(function (result) {
                        fixture.slice(0, 3).forEach(function (data, index) {
                            expect(data['user_id']).to.be(result[index]['user_id']);
                            expect(data['email']).to.be(result[index]['email']);
                            expect(data['name']).to.be(result[index]['name']);
                            expect(data['birthday']).to.be(result[index]['birthday']);
                            expect(data['blood_type']).to.be(result[index]['blood_type']);
                            expect(data['size']).to.eql(result[index]['size']);
                            expect(data['favorite_food']).to.be(result[index]['favorite_food']);
                            expect(data['dislikes_food']).to.be(result[index]['dislikes_food']);
                            expect(data['color']).to.be(result[index]['color']);
                            expect(data['unit']).to.be(result[index]['unit']);
                        });

                        done();
                    }).catch(function (error) {
                        done(error);
                    });
                });

                it('Callback', function (done) {
                    const table = database.table(key),
                        conditions = {
                            user_id: {
                                $in: [1, 2, 3]
                            }
                        };
                    table.find(conditions, function (error, result) {
                        if (error) {
                            return done(error);
                        }

                        fixture.slice(0, 3).forEach(function (data, index) {
                            expect(data['user_id']).to.be(result[index]['user_id']);
                            expect(data['email']).to.be(result[index]['email']);
                            expect(data['name']).to.be(result[index]['name']);
                            expect(data['birthday']).to.be(result[index]['birthday']);
                            expect(data['blood_type']).to.be(result[index]['blood_type']);
                            expect(data['size']).to.eql(result[index]['size']);
                            expect(data['favorite_food']).to.be(result[index]['favorite_food']);
                            expect(data['dislikes_food']).to.be(result[index]['dislikes_food']);
                            expect(data['color']).to.be(result[index]['color']);
                            expect(data['unit']).to.be(result[index]['unit']);
                        });

                        done();
                    });
                });
            });
        });

        describe('set', function () {
            const data = {
                user_id: 10,
                name: 'Yukiho Kosaka',
                email: 'yukiho@μs.jp'
            };

            describe('Should insert item', function () {
                it('Promise', function (done) {
                    const table = database.table(key);

                    table.get({user_id: data.user_id}).then(function (result) {
                        expect(result).to.be(null);

                        return table.set(data);
                    }).then(function (result) {
                        Object.keys(data).forEach(function (key) {
                            expect(data[key]).to.be(result[key]);
                        });

                        return table.get({user_id: data.user_id});
                    }).then(function (result) {
                        Object.keys(data).forEach(function (key) {
                            expect(data[key]).to.be(result[key]);
                        });

                        return new Promise(function (resolve, reject) {
                            table.model.delete({user_id: data.user_id}, function (error, result) {
                                return (error) ? reject(error) : resolve(result);
                            });
                        });
                    }).then(function () {
                        done();
                    }).catch(function (error) {
                        done(error);
                    });
                });

                it('Callback', function (done) {
                    const table = database.table(key);

                    table.get({user_id: data.user_id}).then(function (result) {
                        expect(result).to.be(null);

                        return new Promise(function (resolve, reject) {
                            table.set(data, function (error, result) {
                                return (error) ? reject(error) : resolve(result);
                            });
                        });
                    }).then(function (result) {
                        Object.keys(data).forEach(function (key) {
                            expect(data[key]).to.be(result[key]);
                        });

                        return table.get({user_id: data.user_id});
                    }).then(function (result) {
                        Object.keys(data).forEach(function (key) {
                            expect(data[key]).to.be(result[key]);
                        });

                        return new Promise(function (resolve, reject) {
                            table.model.delete({user_id: data.user_id}, function (error, result) {
                                return (error) ? reject(error) : resolve(result);
                            });
                        });
                    }).then(function () {
                        done();
                    }).catch(function (error) {
                        done(error);
                    });
                });
            });

            describe('Should update item', function () {
                it('Promise', function (done) {
                    const table = database.table(key),
                        updated = Object.assign({}, data, {name: 'Kosaka Yukiho'});

                    table.get({user_id: data.user_id}).then(function (result) {
                        expect(result).to.be(null);

                        return table.set(data);
                    }).then(function (result) {
                        Object.keys(data).forEach(function (key) {
                            expect(data[key]).to.be(result[key]);
                        });

                        result['name'] = updated.name;

                        return table.set(result);
                    }).then(function (result) {
                        Object.keys(updated).forEach(function (key) {
                            expect(updated[key]).to.be(result[key]);
                        });

                        return table.get({user_id: data.user_id});
                    }).then(function (result) {
                        Object.keys(updated).forEach(function (key) {
                            expect(updated[key]).to.be(result[key]);
                        });

                        return new Promise(function (resolve, reject) {
                            table.model.delete({user_id: data.user_id}, function (error, result) {
                                return (error) ? reject(error) : resolve(result);
                            });
                        });
                    }).then(function () {
                        done();
                    }).catch(function (error) {
                        done(error);
                    });
                });

                it('Callback', function (done) {
                    const table = database.table(key),
                        updated = Object.assign({}, data, {name: 'Kosaka Yukiho'});

                    table.get({user_id: data.user_id}).then(function (result) {
                        expect(result).to.be(null);

                        return new Promise(function (resolve, reject) {
                            table.set(data, function (error, result) {
                                return (error) ? reject(error) : resolve(result);
                            });
                        });
                    }).then(function (result) {
                        Object.keys(data).forEach(function (key) {
                            expect(data[key]).to.be(result[key]);
                        });

                        result['name'] = updated.name;

                        return new Promise(function (resolve, reject) {
                            table.set(result, function (error, result) {
                                return (error) ? reject(error) : resolve(result);
                            });
                        });
                    }).then(function (result) {
                        Object.keys(updated).forEach(function (key) {
                            expect(updated[key]).to.be(result[key]);
                        });

                        return table.get({user_id: data.user_id});
                    }).then(function (result) {
                        Object.keys(updated).forEach(function (key) {
                            expect(updated[key]).to.be(result[key]);
                        });

                        return new Promise(function (resolve, reject) {
                            table.model.delete({user_id: data.user_id}, function (error, result) {
                                return (error) ? reject(error) : resolve(result);
                            });
                        });
                    }).then(function () {
                        done();
                    }).catch(function (error) {
                        done(error);
                    });
                });
            });
        });

        describe('remove', function () {
            describe('Should remove item', function () {
                const data = {
                    user_id: 10,
                    name: 'Yukiho Kosaka',
                    email: 'yukiho@μs.jp'
                };

                it('Promise', function (done) {
                    const table = database.table(key);

                    table.set(data).then(function () {
                        return table.get({user_id: data.user_id});
                    }).then(function (result) {
                        Object.keys(data).forEach(function (key) {
                            expect(data[key]).to.be(result[key]);
                        });

                        return table.remove({user_id: data.user_id});
                    }).then(function () {
                        return table.get({user_id: data.user_id});
                    }).then(function (result) {
                        expect(result).to.be(null);

                        done();
                    }).catch(function (error) {
                        done(error);
                    });
                });

                it('Callback', function (done) {
                    const table = database.table(key);

                    table.set(data).then(function () {
                        return table.get({user_id: data.user_id});
                    }).then(function (result) {
                        Object.keys(data).forEach(function (key) {
                            expect(data[key]).to.be(result[key]);
                        });

                        return new Promise(function (resolve, reject) {
                            table.remove({user_id: data.user_id}, function (error, result) {
                                return (error) ? reject(error) : resolve(result);
                            });
                        });
                    }).then(function () {
                        return table.get({user_id: data.user_id});
                    }).then(function (result) {
                        expect(result).to.be(null);

                        done();
                    }).catch(function (error) {
                        done(error);
                    });
                });
            });

            describe('Should not remove item', function () {
                it('Promise', function (done) {
                    const table = database.table(key);

                    table.count().then(function (result) {
                        const count = result;
                        table.remove().then(function() {
                            return table.count();
                        }).then(function (result) {
                            expect(result).to.be(count);

                            done();
                        });
                    });
                });

                it('Callback', function (done) {
                    const table = database.table(key);

                    table.count().then(function (result) {
                        const count = result;

                        table.remove(function (error) {
                            if (error) {
                                return done(error);
                            }

                            table.count().then(function (result) {
                                expect(result).to.be(count);

                                done();
                            });
                        });
                    });
                });
            });
        });


        after(function (done) {
            const connection = database.getConnection(),
                parallels = Object.keys(connection.modelInstance).map(function (key) {
                    return new Promise(function (resolve, reject) {
                        connection.modelInstance[key].drop_table(function (error, result) {
                            return (error) ? reject(error) : resolve(result);
                        });
                    });
                });

            Promise.all(parallels).then(function () {
                done();
            }).catch(function (error) {
                done(error);
            });
        });
    });

    describe('sequence', function() {
        const database = adapter(options);

        before(function (done) {
            database.connect().then(function () {
                done();
            });
        });

        it('Should get sequence', function (done) {
            const sequence = database.sequence('sequence');

            expect(sequence.constructor.name).to.be('KarmiaDatabaseAdapterCassandraSequence');

            done();
        });

        describe('Should get sequence value', function () {
            it('Promise', function (done) {
                const sequence = database.sequence('sequence-promise');

                sequence.get().then(function (result) {
                    expect(result).to.be(1);

                    return sequence.get();
                }).then(function (result) {
                    expect(result).to.be(2);

                    return sequence.get();
                }).then(function (result) {
                    expect(result).to.be(3);

                    return sequence.get();
                }).then(function (result) {
                    expect(result).to.be(4);

                    return sequence.get();
                }).then(function (result) {
                    expect(result).to.be(5);

                    done();
                }).catch(function (error) {
                    done(error);
                })
            });

            it('Callback', function (done) {
                const sequence = database.sequence('sequence-callback');

                sequence.get(function (error, result) {
                    if (error) {
                        return done(error);
                    }

                    expect(result).to.be(1);
                    sequence.get(function (error, result) {
                        if (error) {
                            return done(error);
                        }

                        expect(result).to.be(2);
                        sequence.get(function (error, result) {
                            if (error) {
                                return done(error);
                            }

                            expect(result).to.be(3);
                            sequence.get(function (error, result) {
                                if (error) {
                                    return done(error);
                                }

                                expect(result).to.be(4);
                                sequence.get(function (error, result) {
                                    if (error) {
                                        return done(error);
                                    }

                                    expect(result).to.be(5);

                                    done();
                                });
                            });
                        });
                    });
                });
            });
        });

        after(function (done) {
            const connection = database.getConnection(),
                parallels = Object.keys(connection.modelInstance).map(function (key) {
                    return new Promise(function (resolve, reject) {
                        connection.modelInstance[key].drop_table(function (error, result) {
                            return (error) ? reject(error) : resolve(result);
                        });
                    });
                });

            Promise.all(parallels).then(function () {
                done();
            }).catch(function (error) {
                done(error);
            });
        });
    });

    describe('suite', function () {
        const database = adapter(options),
            key = 'user',
            tables = ['user'];

        before(function (done) {
            database.define(key, schema).sync().then(function () {
                const table = database.table(key);

                return fixture.reduce(function (promise, data) {
                    return promise.then(function () {
                        return table.set(data);
                    });
                }, Promise.resolve());
            }).then(function () {
                done();
            }).catch(function (error) {
                done(error);
            });
        });

        it('Should create table suite', function (done) {
            const suite = database.suite('user', tables, 1);

            expect(suite.constructor.name).to.be('KarmiaDatabaseAdapterCassandraTableSuite');

            done();
        });

        describe('getId', function () {
            it('Should get ID', function (done) {
                const id = 1,
                    suite = database.suite('user', tables, id);

                expect(suite.getId()).to.be(id);

                done();
            });
        });

        describe('get', function () {
            describe('Should get item', function () {
                it('Promose', function (done) {
                    const suite = database.suite('user', tables, 1);
                    suite.get().then(function (result) {
                        const data = fixture[0];

                        expect(data['user_id']).to.be(result['user_id']);
                        expect(data['email']).to.be(result['email']);
                        expect(data['name']).to.be(result['name']);
                        expect(data['birthday']).to.be(result['birthday']);
                        expect(data['blood_type']).to.be(result['blood_type']);
                        expect(data['size']).to.eql(result['size']);
                        expect(data['favorite_food']).to.be(result['favorite_food']);
                        expect(data['dislikes_food']).to.be(result['dislikes_food']);
                        expect(data['color']).to.be(result['color']);
                        expect(data['unit']).to.be(result['unit']);

                        done();
                    }).catch(function (error) {
                        done(error);
                    });
                });

                it('Callback', function (done) {
                    const suite = database.suite('user', tables, 1);
                    suite.get(function (error, result) {
                        if (error) {
                            return done(error);
                        }

                        const data = fixture[0];

                        expect(data['user_id']).to.be(result['user_id']);
                        expect(data['email']).to.be(result['email']);
                        expect(data['name']).to.be(result['name']);
                        expect(data['birthday']).to.be(result['birthday']);
                        expect(data['blood_type']).to.be(result['blood_type']);
                        expect(data['size']).to.eql(result['size']);
                        expect(data['favorite_food']).to.be(result['favorite_food']);
                        expect(data['dislikes_food']).to.be(result['dislikes_food']);
                        expect(data['color']).to.be(result['color']);
                        expect(data['unit']).to.be(result['unit']);

                        done();
                    });
                });
            });
        });

        describe('set', function () {
            const data = {
                user_id: 10,
                name: 'Yukiho Kosaka',
                email: 'yukiho@μs.jp'
            };

            describe('Should insert item', function () {
                it('Promise', function (done) {
                    const suite = database.suite('user', tables, data.user_id);
                    suite.get().then(function (result) {
                        expect(result).to.be(null);

                        return suite.set(data);
                    }).then(function (result) {
                        Object.keys(data).forEach(function (key) {
                            expect(data[key]).to.be(result[key]);
                        });

                        return suite.get();
                    }).then(function (result) {
                        Object.keys(data).forEach(function (key) {
                            expect(data[key]).to.be(result[key]);
                        });

                        return suite.remove();
                    }).then(function () {
                        done();
                    }).catch(function (error) {
                        done(error);
                    });
                });

                it('Callback', function (done) {
                    const suite = database.suite('user', tables, data.user_id);
                    suite.get().then(function (result) {
                        expect(result).to.be(null);

                        return new Promise(function (resolve, reject) {
                            suite.set(data, function (error, result) {
                                return (error) ? reject(error) : resolve(result);
                            });
                        });
                    }).then(function (result) {
                        Object.keys(data).forEach(function (key) {
                            expect(data[key]).to.be(result[key]);
                        });

                        return suite.get();
                    }).then(function (result) {
                        Object.keys(data).forEach(function (key) {
                            expect(data[key]).to.be(result[key]);
                        });

                        return suite.remove();
                    }).then(function () {
                        done();
                    }).catch(function (error) {
                        done(error);
                    });
                });
            });

            describe('Should update item', function () {
                it('Promise', function (done) {
                    const suite = database.suite('user', tables, data.user_id),
                        updated = Object.assign({}, data, {name: 'Kosaka Yukiho'});
                    suite.get().then(function (result) {
                        expect(result).to.be(null);

                        return suite.set(data);
                    }).then(function (result) {
                        Object.keys(data).forEach(function (key) {
                            expect(data[key]).to.be(result[key]);
                        });

                        result['name'] = updated.name;

                        return suite.set(result);
                    }).then(function (result) {
                        Object.keys(updated).forEach(function (key) {
                            expect(updated[key]).to.be(result[key]);
                        });

                        return suite.get();
                    }).then(function (result) {
                        Object.keys(updated).forEach(function (key) {
                            expect(updated[key]).to.be(result[key]);
                        });

                        return suite.remove({user_id: data.user_id});
                    }).then(function () {
                        done();
                    }).catch(function (error) {
                        done(error);
                    });
                });

                it('Callback', function (done) {
                    const suite = database.suite('user', tables, data.user_id),
                        updated = Object.assign({}, data, {name: 'Kosaka Yukiho'});
                    suite.get().then(function (result) {
                        expect(result).to.be(null);

                        return new Promise(function (resolve, reject) {
                            suite.set(data, function (error, result) {
                                return (error) ? reject(error) : resolve(result);
                            });
                        });
                    }).then(function (result) {
                        Object.keys(data).forEach(function (key) {
                            expect(data[key]).to.be(result[key]);
                        });

                        result['name'] = updated.name;

                        return new Promise(function (resolve, reject) {
                            suite.set(result, function (error, result) {
                                return (error) ? reject(error) : resolve(result);
                            });
                        });
                    }).then(function (result) {
                        Object.keys(updated).forEach(function (key) {
                            expect(updated[key]).to.be(result[key]);
                        });

                        return suite.get();
                    }).then(function (result) {
                        Object.keys(updated).forEach(function (key) {
                            expect(updated[key]).to.be(result[key]);
                        });

                        return suite.remove();
                    }).then(function () {
                        done();
                    }).catch(function (error) {
                        done(error);
                    });
                });
            });
        });

        describe('remove', function () {
            describe('Should remove item', function () {
                const data = {
                    user_id: 10,
                    name: 'Yukiho Kosaka',
                    email: 'yukiho@μs.jp'
                };

                it('Promise', function (done) {
                    const suite = database.suite('user', tables, data.user_id);
                    suite.set(data).then(function () {
                        return suite.get();
                    }).then(function (result) {
                        Object.keys(data).forEach(function (key) {
                            expect(data[key]).to.be(result[key]);
                        });

                        return suite.remove();
                    }).then(function () {
                        return suite.get();
                    }).then(function (result) {
                        expect(result).to.be(null);

                        done();
                    }).catch(function (error) {
                        done(error);
                    });
                });

                it('Callback', function (done) {
                    const suite = database.suite('user', tables, data.user_id);
                    suite.set(data).then(function () {
                        return suite.get();
                    }).then(function (result) {
                        Object.keys(data).forEach(function (key) {
                            expect(data[key]).to.be(result[key]);
                        });

                        return new Promise(function (resolve, reject) {
                            suite.remove(function (error, result) {
                                return (error) ? reject(error) : resolve(result);
                            });
                        });
                    }).then(function () {
                        return suite.get();
                    }).then(function (result) {
                        expect(result).to.be(null);

                        done();
                    }).catch(function (error) {
                        done(error);
                    });
                });
            });
        });

        after(function (done) {
            const connection = database.getConnection(),
                parallels = Object.keys(connection.modelInstance).map(function (key) {
                    return new Promise(function (resolve, reject) {
                        connection.modelInstance[key].drop_table(function (error, result) {
                            return (error) ? reject(error) : resolve(result);
                        });
                    });
                });

            Promise.all(parallels).then(function () {
                done();
            }).catch(function (error) {
                done(error);
            });
        });
    });
});



/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * c-hanging-comment-ender-p: nil
 * End:
 */
