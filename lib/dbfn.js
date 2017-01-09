var _ = require('lodash');

module.exports = reference => {
    return {
        table : {
            getData: parameters => {
                function mountQuery(tableMetadata) {
                    if(parameters.columns) {
                        for(let i in parameters.columns) { // remove unexpected columns (weird syntax or not present)
                            let c = parameters.columns[i];
                            if((!(/^[0-9a-zA-Z_]*$/).test(c)) || _.findIndex(tableMetadata, t => t.COLUMN_NAME == c) === -1) {
                                console.log('removed: ' + c);
                                parameters.columns.splice(i, 1);
                            }
                        }

                        if(parameters.columns.length === 0) delete parameters.columns;
                    }

                    // limit
                    var limit = "";
                    if(reference.baseName === 'mssql' && parameters.limit && !isNaN(parameters.limit)) limit = "TOP " + parameters.limit + " ";

                    reference.fn.sql = "SELECT " + limit + (parameters.columns || '*') + " FROM " + reference.fn.table.name;

                    if(parameters.where) {
                        for(let c in parameters.where) { // remove unexpected columns (weird syntax or not present)
                            var column = (/([a-zA-Z_]*)( [a-zA-Z_]*)?/).exec(c)[1]
                            if((!(/^[a-zA-Z_]*$/).test(column)) || _.findIndex(tableMetadata, t => t.COLUMN_NAME == column) === -1) {
                                delete parameters.where[c];
                            }
                        }

                        var whereColumns = Object.keys(parameters.where);

                        if(whereColumns.length) {
                            reference.fn.where = {};
                            reference.fn.where.columns = []
                            reference.fn.sql += " WHERE ";
                            for (var i = 0; i < whereColumns.length; i++) {
                                let column = (/([a-zA-Z_]*)( [a-zA-Z_]*)?/).exec(whereColumns[i])[1];
                                let signal = parameters.where[whereColumns[i]] === null ?  ' IS ' : "="; // standard

                                reference.fn.where.columns.push(column);

                                if((/^([a-zA-Z_]*) (not|NOT)$/).test(whereColumns[i])) { signal = parameters.where[whereColumns[i]] === null ?  ' IS NOT ' : "!="; }
                                if((/^([a-zA-Z_]*) (like|LIKE)$/).test(whereColumns[i])) { signal = " LIKE "; }
                                if((/^([a-zA-Z_]*) (not like|NOT LIKE)$/).test(whereColumns[i])) { signal = " NOT LIKE "; }

                                reference.fn.sql += i > 0 ? " AND " : "";

                                if(reference.baseName === 'mysql') reference.fn.sql += column + signal + "?";
                                if(reference.baseName === 'mssql') reference.fn.sql += column + signal + "@" + column;
                            }

                            reference.fn.where.values = (() => {
                                var values = [];
                                for (var i = 0; i < whereColumns.length; i++) {
                                    values.push(parameters.where[whereColumns[i]]);
                                }
                                return values;
                            })()
                        }
                    }

                    if(parameters.order) {
                        orderColumns = [];
                        for(let i in parameters.order) { // remove unexpected columns (weird syntax or not present)
                            let c = (/([a-zA-Z_]*)( [a-zA-Z_]*)?/).exec(parameters.order[i])[1];
                            if(_.findIndex(tableMetadata, t => t.COLUMN_NAME == c) === -1) {
                                parameters.order.splice(i, 1);
                            }

                            if((/^([a-zA-Z_]+) (desc|DESC)$/).test(parameters.order[i])) c += ' DESC';
                            orderColumns.push(c);
                        }

                        reference.fn.sql += " ORDER BY " + orderColumns.toString();
                    }

                    if(reference.baseName === 'mysql' && parameters.limit && !isNaN(parameters.limit)) limit = " LIMIT " + parameters.limit;
                    reference.fn.sql += limit;

                    console.log(reference.fn.sql);
                }

                switch(reference.baseName) {
                    case 'mysql': {
                        return new Promise((resolve, reject) => {
                            var conn = reference.base.createConnection(reference.databaseOptions);

                            if(parameters && (parameters.columns || parameters.where || parameters.group || parameters.order)) {
                                conn.query({
                                    sql: `SELECT COLUMN_NAME
                                    FROM information_schema.columns
                                    WHERE TABLE_SCHEMA=?
                                    AND TABLE_NAME=?`,
                                    timeout: reference.databaseOptions.timeout || 5000
                                }, [reference.databaseOptions.database, reference.fn.table.name], (error, results, fields) => { // information_schema - table columns info
                                    if(error) { conn.end(); reject('(SQL Error) ' + error); return; }
                                    mountQuery(results);

                                    conn.query({ sql: reference.fn.sql, timeout: reference.databaseOptions.timeout || 5000 },
                                        (reference.fn.where && reference.fn.where.values ? reference.fn.where.values : []), (error, results, fields) => {
                                        if(error) { conn.end(); reject('(SQL Error) ' + error); return; }

                                        conn.end();
                                        resolve(results);
                                        return;
                                    });
                                });
                            }
                            else {
                                conn.query({ sql: 'SELECT * FROM ' + reference.fn.table.name, timeout: 4000 }, (error, results, fields) => {
                                    if(error) {
                                        conn.end();
                                        reject(Error('Error on connection: ' + error));
                                        return;
                                    }

                                    conn.end();
                                    resolve(results);
                                    return;
                                });
                            }
                        })
                    }

                    case 'mssql': {
                        return new Promise((resolve, reject) => {
                            var db = new reference.base.Connection(reference.databaseOptions);
                            db.connect(error => {
                                if(error) { db.close(); reject('Error on connection: ' + error.stack); return; }

                                if(parameters && (parameters.columns || parameters.where || parameters.group || parameters.order)) {
                                    var requestMetadata = db.request();
                                    var rsMetadata = { rows: [] }
                                    requestMetadata.input('TABLE_CATALOG', reference.databaseOptions.database    );
                                    requestMetadata.input('TABLE_NAME', reference.fn.table.name);
                                    requestMetadata.query(`SELECT COLUMN_NAME
                                        FROM INFORMATION_SCHEMA.COLUMNS
                                        WHERE TABLE_CATALOG = @TABLE_CATALOG AND TABLE_NAME = @TABLE_NAME`);

                                    requestMetadata.on('error', error => { reject('(SQL Error) ' + error.stack);  });
                                    requestMetadata.on('row', row => { rsMetadata.rows.push(row); });
                                    requestMetadata.on('done', (affected) => {
                                        console.log(rsMetadata.rows);
                                        mountQuery(rsMetadata.rows);

                                        var request = db.request();
                                        var rs = { rows: [] }

                                        if(reference.fn.where.columns) {
                                            for(var i in reference.fn.where.columns) {
                                                request.input(reference.fn.where.columns[i], reference.fn.where.values[i]);
                                            }
                                        }
                                        request.query(reference.fn.sql);
                                        request.on('error', error => { reject('(SQL Error) ' + error.stack);  });
                                        // request.on('recordset', columns => { rs.columns = columns;  });
                                        request.on('row', row => { rs.rows.push(row); });
                                        request.on('done', (affected) => {
                                            db.close();
                                            resolve(rs.rows);
                                            return;
                                        });
                                    });
                                }
                                else {
                                    var request = db.request();
                                    var rs = { rows: [] }
                                    request.query('SELECT * FROM ' + reference.fn.table.name);
                                    request.on('error', error => { reject('(SQL Error) ' + error.stack);  });
                                    // request.on('recordset', columns => { rs.columns = columns;  });
                                    request.on('row', row => { rs.rows.push(row); });
                                    request.on('done', (affected) => {
                                        db.close();
                                        resolve(rs.rows);
                                        return;
                                    });
                                }
                            })
                        })
                    }
                }
            },
            setData: object => {
                function validateObject(tableMetadata) {
                    reference.fn.faultValues = [];

                    for(var c in object) { // remove unknown columns
                        if(_.findIndex(tableMetadata, tc => tc.COLUMN_NAME === c) === -1) delete object[c];
                    }

                    var requiredColumns = _.filter(tableMetadata, c => c.IS_NULLABLE === 'NO');

                    for(var i in requiredColumns) {
                        let column = requiredColumns[i];
                        if(_.findIndex(Object.keys(object), k => k === column.COLUMN_NAME) === -1) {
                            if(column.EXTRA !== 'auto_increment') {
                                reference.fn.faultValues.push({ column: column.COLUMN_NAME, issue: 'required' });
                            }
                        }
                    }

                    if(reference.fn.faultValues.length > 0) return;

                    for(var i in tableMetadata) {
                        let column = tableMetadata[i];

                        if(object[column.COLUMN_NAME]) {
                            if((/^(int|float|double|float|decimal)\(?\d+?\)?$/).test(column.COLUMN_TYPE.toLowerCase())) {
                                if(isNaN(object[column.COLUMN_NAME])) {
                                    reference.fn.faultValues.push({ column: column.COLUMN_NAME, issue: 'NaN' })
                                }
                            }
                            else if( (/^([varchar]+)(?:\((\d+)\))?$/).test(column.COLUMN_TYPE.toLowerCase())) {
                                let t = (/^([varchar]+)(?:\((\d+)\))?$/).exec(column.COLUMN_TYPE.toLowerCase());
                                let typeName = t[1];
                                let typeLength = t[2];
                                if(object[column.COLUMN_NAME].length > typeLength) {
                                    reference.fn.faultValues.push({ column: column.COLUMN_NAME, issue: 'overflow'  })
                                }
                            }
                        }
                    }
                }

                switch(reference.baseName) {
                    case 'mysql': {
                        return new Promise((resolve, reject) => {
                            var conn = reference.base.createConnection(reference.databaseOptions);
                            conn.query({
                                sql: `SELECT COLUMN_NAME, COLUMN_TYPE, CHARACTER_MAXIMUM_LENGTH, COLUMN_KEY, EXTRA, IS_NULLABLE
                                FROM information_schema.columns
                                WHERE TABLE_SCHEMA=?
                                AND TABLE_NAME=?`,
                                timeout: reference.databaseOptions.timeout || 5000
                            }, [reference.databaseOptions.database, reference.fn.table.name], (error, results, fields) => { // information_schema - table columns info
                                if(error) { conn.end(); reject('(SQL Error) ' + error); return; }
                                validateObject(results);
                                if(reference.fn.faultValues.length > 0) {
                                    conn.end();
                                    reject(reference.fn.faultValues);
                                    return;
                                }

                                console.log(_.findIndex(results, r => r.COLUMN_KEY === 'PRI') !== -1); // if it has primary key

                                conn.end();
                                resolve(':)');
                                return;

                                // conn.query({ sql: "INSERT INTO people SET ?", timeout: reference.databaseOptions.timeout || 5000 },
                                //     { name:'Steve Jobs', birthdate: '1955-sd' }, (error, results, fields) => {
                                //     if(error) { conn.end(); reject('(SQL Error) ' + error); return; }
                                //
                                //     conn.end();
                                //     resolve(results);
                                //     return;
                                // });
                            });
                        })
                    }
                    case 'mssql': {
                        return new Promise((resolve, reject) => {
                            var db = new reference.base.Connection(reference.databaseOptions);
                            db.connect(error => {
                                if(error) { db.close(); reject('Error on connection: ' + error.stack); return; }

                                var requestMetadata = db.request();
                                var rsMetadata = { rows: [] }
                                requestMetadata.input('TABLE_CATALOG', reference.databaseOptions.database    );
                                requestMetadata.input('TABLE_NAME', reference.fn.table.name);
                                requestMetadata.query(`SELECT C.COLUMN_NAME, CONCAT(C.DATA_TYPE,'(', C.CHARACTER_MAXIMUM_LENGTH,')') AS COLUMN_TYPE, C.CHARACTER_MAXIMUM_LENGTH,
                                    (CASE WHEN PKS.COLUMN_NAME IS NOT NULL THEN 'PRI' ELSE '' END) AS COLUMN_KEY,
                                    (CASE WHEN X.COLUMN_NAME IS NOT NULL THEN 'auto_increment' ELSE '' END) AS EXTRA, C.IS_NULLABLE
                                    FROM INFORMATION_SCHEMA.COLUMNS C
                                    LEFT JOIN
                                    (
                                    SELECT TABLE_NAME, COLUMN_NAME
                                    FROM INFORMATION_SCHEMA.COLUMNS
                                    WHERE COLUMNPROPERTY(object_id(TABLE_SCHEMA+'.'+TABLE_NAME), COLUMN_NAME, 'isIdentity') =1
                                    ) AS X ON C.TABLE_NAME=X.TABLE_NAME AND C.COLUMN_NAME=X.COLUMN_NAME
                                    LEFT JOIN
                                    (
                                    SELECT TABLE_NAME, COLUMN_NAME
                                    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                                    ) AS PKS ON C.TABLE_NAME=PKS.TABLE_NAME AND C.COLUMN_NAME=PKS.COLUMN_NAME
                                    WHERE C.TABLE_CATALOG=@TABLE_CATALOG AND C.TABLE_NAME=@TABLE_NAME`);

                                requestMetadata.on('error', error => { reject('(SQL Error) ' + error.stack);  });
                                requestMetadata.on('row', row => { rsMetadata.rows.push(row); });
                                requestMetadata.on('done', (affected) => {
                                    validateObject(rsMetadata.rows);
                                    if(reference.fn.faultValues.length > 0) {
                                        db.close();
                                        reject(reference.fn.faultValues);
                                        return;
                                    }

                                    db.close();
                                    resolve(':)');
                                    return;

                                    // var request = db.request();
                                    // var rs = { rows: [] }
                                    //
                                    // if(reference.fn.where.columns) {
                                    //     for(var i in reference.fn.where.columns) {
                                    //         request.input(reference.fn.where.columns[i], reference.fn.where.values[i]);
                                    //     }
                                    // }
                                    // request.query(reference.fn.sql);
                                    // request.on('error', error => { reject('(SQL Error) ' + error.stack);  });
                                    // // request.on('recordset', columns => { rs.columns = columns;  });
                                    // request.on('row', row => { rs.rows.push(row); });
                                    // request.on('done', (affected) => {
                                    //     db.close();
                                    //     resolve(rs.rows);
                                    //     return;
                                    // });
                                });
                            })
                        })
                    }
                }
            }
        }
    }
}
