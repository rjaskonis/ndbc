const _ = require('lodash');
const moment = require('moment');


module.exports = reference => {
    return {
        table: TABLE_NAME => {
            var fn = { };
            return {
                getData: parameters => {
                    const mountQuery = tableMetadata => {
                        var query_joins = {
                            bindings: '',
                            columns: ''
                        };

                        if(parameters.joins && parameters.joins.length) {
                            parameters.joins.forEach((join, index)=> {
                                joinTableMetadata = _.filter(tableMetadata, md => md.TABLE_NAME === join.table.name);

                                if(join.type && join.type.toLowerCase() === 'left') { join.type = 'LEFT'; }
                                else if(join.type && join.type.toLowerCase() === 'right') { join.type = 'RIGHT'; }
                                else { join.type = 'INNER'; }

                                join.on.forEach((link, linkIndex) => {
                                    if(
                                        link.foreignColumn && link.column &&
                                        (
                                            !(/^[0-9a-zA-Z_]*$/).test(link.foreignColumn) ||
                                            _.findIndex(tableMetadata, md => md.COLUMN_NAME == link.foreignColumn) === -1 ||
                                            !(/^[0-9a-zA-Z_]*$/).test(link.column) ||
                                            _.findIndex(tableMetadata, md => md.COLUMN_NAME == link.column) === -1
                                        )
                                    )
                                    {
                                        join.on.splice(linkIndex, 1);
                                    }
                                })

                                if(join.on.length) {
                                    query_joins.bindings += ' ' + join.type + ' JOIN ' + join.table.name + ' AS ' + join.table.alias + ' ON ';

                                    join.on.forEach((link, linkIndex) => {
                                        if(link.foreignColumn && link.column && !link.where) {
                                            if(linkIndex > 0)  query_joins.bindings += ' AND ';
                                            query_joins.bindings += join.table.alias + '.' + link.column + '=' + (link.alias || TABLE_NAME) + '.' + link.foreignColumn;
                                        }

                                        if(link.where && Object.keys(link.where).length) {
                                            Object.keys(link.where).forEach((key, keyIndex) => {
                                                var column = (/([0-9a-zA-Z_]*)( [0-9a-zA-Z_]*)?/).exec(key)[1]
                                                if(
                                                    (/^[0-9a-zA-Z_]*$/).test(column) &&
                                                    _.findIndex(tableMetadata, md => md.COLUMN_NAME == column) !== -1  &&
                                                    ((/^[0-9a-zA-Z_-\s\*]*$/).test(link.where[key]) || link.where[key] === null)
                                                ) {
                                                    if(linkIndex > 0)  query_joins.bindings += ' AND ';
                                                    let signal = link.where[key] === null ? ' IS ' : '=';

                                                    if((/^([0-9a-zA-Z_]*) (not|NOT)$/).test(key)) {
                                                        signal = link.where[key] === null ? ' IS NOT ' : '!=';
                                                    }

                                                    query_joins.bindings += join.table.alias + '.' + column + signal + (link.where[key] ? "'" + link.where[key] + "'" : 'NULL');
                                                }
                                            })
                                        }
                                    })
                                }

                                if(join.table.columns && join.table.columns.constructor === Array) {
                                    join.table.columns.forEach((column, index) => {
                                        if((!(/^[0-9a-zA-Z_]*$/).test(column)) || _.findIndex(joinTableMetadata, md => md.COLUMN_NAME == column) === -1) {
                                            console.log('removed column:' + column);
                                            join.table.columns.splice(index, 1);
                                        }
                                        else {
                                            join.table.columns[index] = join.table.alias + "." + column + ' AS \'' + join.table.alias + '.' + column + '\'';
                                        }
                                    })

                                    if(join.table.columns.length === 0) delete join.table.columns;
                                }
                                else {
                                    delete join.table.columns;
                                }

                                if(index) query_joins.columns += ',';

                                if(join.table.columns && join.table.columns.length) {
                                    query_joins.columns += join.table.columns.toString();
                                }
                                else {
                                    let joinedTableColumns = [];
                                    joinTableMetadata.forEach(md => {
                                        joinedTableColumns.push(join.table.alias + '.' + md.COLUMN_NAME + ' AS \'' + join.table.alias + '.' + md.COLUMN_NAME + '\'');
                                    })
                                    query_joins.columns += joinedTableColumns.toString();
                                }
                            }); // end each join

                            // console.log(query_joins); // ****JOINS**** //


                            parameters.joins = []; // reset joins for further assistence
                        } // if has joins

                        if(!parameters) parameters = {};

                        if(parameters.columns && parameters.columns.constructor === Array) {
                            for(let i in parameters.columns) { // remove unexpected columns (weird syntax or not present)
                                let c = parameters.columns[i];
                                if((!(/^[0-9a-zA-Z_]*$/).test(c)) || _.findIndex(tableMetadata, t => t.COLUMN_NAME == c) === -1) {
                                    parameters.columns.splice(i, 1);
                                }
                                else {
                                    parameters.columns[i] = TABLE_NAME + '.' + parameters.columns[i];
                                }
                            }

                            if(parameters.columns.length === 0) delete parameters.columns;
                        }
                        else {
                            var mainTableColumns = [];
                            _.filter(tableMetadata, md => md.TABLE_NAME === TABLE_NAME).forEach(md => {
                                mainTableColumns.push(TABLE_NAME + '.' + md.COLUMN_NAME);
                            })
                            parameters.columns = mainTableColumns;
                            // delete parameters.columns;
                        }

                        // limit
                        var limit = "";
                        if(reference.base.name === 'mssql' && parameters.limit && !isNaN(parameters.limit)) limit = "TOP " + parameters.limit + " ";

                        fn.sql = "SELECT " + limit + (parameters.columns || (query_joins.columns ? '' : '*')) + (parameters.columns && query_joins.columns ? ',' : '') + (query_joins.columns ? query_joins.columns : '') + " FROM " + TABLE_NAME + query_joins.bindings;

                        if(parameters.where && parameters.where != 'undefined') {
                            for(let c in parameters.where) { // remove unexpected columns (weird syntax or not present)
                                var column = (/([0-9a-zA-Z_]*)( [0-9a-zA-Z_]*)?/).exec(c)[1]
                                if((!(/^[0-9a-zA-Z_]*$/).test(column)) || _.findIndex(tableMetadata, t => t.COLUMN_NAME == column) === -1) {
                                    delete parameters.where[c];
                                }
                            }

                            var whereColumns = Object.keys(parameters.where);

                            if(whereColumns.length) {
                                fn.where = {};
                                fn.where.columns = [];
                                fn.sql += " WHERE ";
                                for (let i = 0; i < whereColumns.length; i++) {
                                    let column = (/([0-9a-zA-Z_]*)( [0-9a-zA-Z_]*)?/).exec(whereColumns[i])[1];
                                    let signal = parameters.where[whereColumns[i]] === null ?  ' IS ' : "="; // standard

                                    if(parameters.where[whereColumns[i]] !== null){
                                        fn.where.columns.push(column);
                                    }

                                    if((/^([0-9a-zA-Z_]*) (not|NOT)$/).test(whereColumns[i])) { signal = parameters.where[whereColumns[i]] === null ?  ' IS NOT ' : "!="; }
                                    if((/^([0-9a-zA-Z_]*) (like|LIKE)$/).test(whereColumns[i])) { signal = " LIKE "; }
                                    if((/^([0-9a-zA-Z_]*) (not like|NOT LIKE)$/).test(whereColumns[i])) { signal = " NOT LIKE "; }

                                    fn.sql += i > 0 ? " AND " : "";

                                    // if(reference.base.name === 'mysql') fn.sql += column + signal + "?";
                                    // if(reference.base.name === 'mssql') fn.sql += column + signal + "@" + column;
                                    if(reference.base.name === 'mysql') fn.sql += TABLE_NAME + '.' + column + signal + (parameters.where[whereColumns[i]] === null ? "NULL" : "?");
                                    if(reference.base.name === 'mssql') fn.sql += TABLE_NAME + '.' + column + signal + (parameters.where[whereColumns[i]] === null ? "NULL" : ("@" + column));
                                }

                                fn.where.values = (() => {
                                    var values = [];
                                    for (let i = 0; i < whereColumns.length; i++) {
                                        if(parameters.where[whereColumns[i]] !== null) {
                                            values.push(parameters.where[whereColumns[i]]);
                                        }
                                    }
                                    return values;
                                })()
                            }
                        }

                        if(parameters.order && parameters.order != 'undefined') {
                            orderColumns = [];
                            for(let i in parameters.order) { // remove unexpected columns (weird syntax or not present)
                                let c = (/([a-zA-Z\d_]*)( [a-zA-Z\d_]*)?/).exec(parameters.order[i])[1];
                                if(_.findIndex(tableMetadata, t => t.COLUMN_NAME == c) === -1) {
                                    parameters.order.splice(i, 1);
                                }

                                if((/^([a-zA-Z\d_]+) (desc|DESC)$/).test(parameters.order[i])) c += ' DESC';
                                orderColumns.push(c);
                            }

                            fn.sql += " ORDER BY " + orderColumns.toString();
                        }

                        if(reference.base.name === 'mysql' && parameters.limit && !isNaN(parameters.limit)) limit = " LIMIT " + parameters.limit;
                        fn.sql += limit;
                    } // mountQuery

                    switch(reference.base.name) {
                        case 'mysql': {
                            return new Promise((resolve, reject) => {
                                var conn = reference.base.createConnection(reference.databaseOptions);
                                var tables = [TABLE_NAME];
                                var query_tables = 'TABLE_NAME=?';
                                if(parameters && parameters.joins){
                                    parameters.joins.forEach(join => { query_tables += ' OR TABLE_NAME=?'; tables.push(join.table.name) })
                                }

                                if(parameters && (parameters.columns || parameters.where || parameters.group || parameters.order || parameters.datetimeFormat)) {
                                    conn.query({
                                        sql: `SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE
                                        FROM information_schema.columns
                                        WHERE TABLE_SCHEMA=?
                                        AND (` + query_tables + `)`,
                                        timeout: reference.databaseOptions.timeout || 5000
                                    }, [reference.databaseOptions.database, ...tables], (errorMD, resultsMD, fieldsMD) => { // information_schema - table columns info
                                        if(errorMD) { conn.end(); reject('(SQL Error) ' + errorMD); return; }
                                        mountQuery(resultsMD);
                                        // console.log(fn.sql);

                                        conn.query({ sql: fn.sql, timeout: reference.databaseOptions.timeout || 5000 },
                                            (fn.where && fn.where.values ? fn.where.values : []), (error, results, fields) => {
                                                if(error) { conn.end(); reject('(SQL Error) ' + error); return; }

                                                // PARSING
                                                blobIndexes = [];
                                                datetimeIndexes = [];
                                                for(let i in resultsMD) {
                                                    if((/blob/).test(resultsMD[i].COLUMN_TYPE)) {
                                                        blobIndexes.push(i);
                                                    }
                                                    if(parameters.datetimeFormat && (/datetime/).test(resultsMD[i].COLUMN_TYPE)) {
                                                        datetimeIndexes.push(i);
                                                    }
                                                }
                                                if(blobIndexes.length) { // BLOB PARSING
                                                    for(let i in results) {
                                                        for(let j in blobIndexes){
                                                            let blobIndex = blobIndexes[j]; // column index

                                                            let columnName = Object.keys(results[i]).find(column => column.indexOf(resultsMD[blobIndex].COLUMN_NAME) > -1);

                                                            if(results[i][columnName]) {
                                                                results[i][columnName] = new Buffer(results[i][columnName]).toString('base64');
                                                            }
                                                            // else {
                                                            //     // console.log(results[i]);
                                                            //     console.log('Unparsed blob: ', resultsMD[blobIndex].COLUMN_NAME);
                                                            // }
                                                        }
                                                    }
                                                }
                                                if(datetimeIndexes.length) { // DATETIME PARSING (formating)
                                                    for(let i in results) {
                                                        for(let j in datetimeIndexes){
                                                            let datetimeIndex = datetimeIndexes[j]; // column index

                                                            let columnName = Object.keys(results[i]).find(column => column.indexOf(resultsMD[datetimeIndex].COLUMN_NAME) > -1);

                                                            if(results[i][columnName]) {
                                                                results[i][columnName] = moment(results[i][columnName], 'YYYY-MM-DD').format(parameters.datetimeFormat);
                                                            }
                                                        }
                                                    }
                                                }
                                                //

                                                conn.end();
                                                resolve(results);
                                                return;
                                            });
                                        });
                                    }
                                    else {
                                        conn.query({ sql: 'SELECT * FROM ' + TABLE_NAME, timeout: 4000 }, (error, results, fields) => {
                                            if(error) { conn.end(); reject('(SQL Error) ' + error); return; }

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
                                var tables = [TABLE_NAME];
                                var query_tables = 'TABLE_NAME=@TABLE_NAME';
                                if(parameters && parameters.joins){
                                    parameters.joins.forEach((join, index) => { query_tables += (' OR TABLE_NAME=@TABLE_NAME' + index); tables.push(join.table.name) })
                                }

                                db.connect(error => {
                                    if(error) { db.close(); reject('Error on connection: ' + error.stack); return; }

                                    if(parameters && (parameters.columns || parameters.where || parameters.group || parameters.order)) {
                                        var requestMetadata = db.request();
                                        var rsMetadata = { rows: [] }
                                        requestMetadata.input('TABLE_CATALOG', reference.databaseOptions.database);
                                        requestMetadata.input('TABLE_NAME', TABLE_NAME);
                                        if(parameters && parameters.joins){
                                            parameters.joins.forEach((join, index) => { requestMetadata.input(('TABLE_NAME' + index), join.table.name) });
                                        }

                                        requestMetadata.query(`
                                            SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE
                                            FROM INFORMATION_SCHEMA.COLUMNS
                                            WHERE TABLE_CATALOG = @TABLE_CATALOG AND (` + query_tables + `)`);

                                        requestMetadata.on('error', error => { reject('(SQL Error) ' + error.stack);  });
                                        requestMetadata.on('row', row => { rsMetadata.rows.push(row); });
                                        requestMetadata.on('done', (affected) => {
                                            mountQuery(rsMetadata.rows);

                                            var request = db.request();
                                            var rs = { rows: [] }

                                            if(fn.where && fn.where.columns) {
                                                for(let i in fn.where.columns) {
                                                    request.input(fn.where.columns[i], fn.where.values[i]);
                                                }
                                            }

                                            // console.log(fn.sql);
                                            request.query(fn.sql);
                                            request.on('error', error => { reject('(SQL Error) ' + error.stack);  });
                                            // request.on('recordset', columns => { rs.columns = columns;  });
                                            request.on('row', row => { rs.rows.push(row); });
                                            request.on('done', (affected) => {
                                                // PARSING
                                                datetimeIndexes = [];
                                                for(let i in rsMetadata.rows) {
                                                    if(parameters.datetimeFormat && (/datetime/).test(rsMetadata.rows[i].DATA_TYPE)) {
                                                        datetimeIndexes.push(i);
                                                    }
                                                }
                                                if(datetimeIndexes.length) { // DATETIME PARSING (formating)
                                                    for(let i in rs.rows) {
                                                        for(let j in datetimeIndexes){
                                                            let datetimeIndex = datetimeIndexes[j]; // column index

                                                            let columnName = Object.keys(rs.rows[i]).find(column => column.indexOf(rsMetadata.rows[datetimeIndex].COLUMN_NAME) > -1);

                                                            if(rs.rows[i][columnName]) {
                                                                rs.rows[i][columnName] = moment(rs.rows[i][columnName], 'YYYY-MM-DD').format(parameters.datetimeFormat);
                                                            }
                                                        }
                                                    }
                                                }

                                                db.close();
                                                resolve(rs.rows);
                                                return;
                                            });
                                        });
                                    }
                                    else {
                                        var request = db.request();
                                        var rs = { rows: [] }
                                        request.query('SELECT * FROM ' + TABLE_NAME);
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
                setData: (object, parameters) => {
                    function parseObject(tableMetadata) {
                        fn.faultValues = [];

                        for(var k in object) { // remove unknown columns
                            if(_.findIndex(tableMetadata, c => c.COLUMN_NAME === k) === -1) delete object[k];
                        }

                        var requiredColumns = _.filter(tableMetadata, c => c.IS_NULLABLE === 'NO');

                        for(let i in requiredColumns) {
                            let column = requiredColumns[i];
                            // if column is found in sent 'object'
                            if((_.findIndex(Object.keys(object), k => k === column.COLUMN_NAME) === -1 ||
                                (/^([varchar]+)([\(\)\d]*)?$/).test(column.COLUMN_TYPE.toLowerCase()) ||
                                (/^([datetime]+)$/).test(column.COLUMN_TYPE.toLowerCase())) &&
                                !object[column.COLUMN_NAME])
                                 {
                                if(column.EXTRA !== 'auto_increment') {
                                    fn.faultValues.push({ column: column.COLUMN_NAME, issue: 'required' });
                                }
                            }
                        }

                        for(let i in tableMetadata) { // parse types and check nullables
                            let column = tableMetadata[i];

                            if(object[column.COLUMN_NAME] || object[column.COLUMN_NAME] === '') { // ignore nulls
                                if((/^(int|float|double|float|decimal)([\(\)\d,]*)?$/).test(column.COLUMN_TYPE.toLowerCase())) {
                                    if(isNaN(object[column.COLUMN_NAME]) ||  object[column.COLUMN_NAME] === '') {
                                        fn.faultValues.push({ column: column.COLUMN_NAME, issue: 'NaN' })
                                    }
                                }
                                else if( (/^([varchar]+)([\(\)\d]*)?$/).test(column.COLUMN_TYPE.toLowerCase())) {
                                    let t = (/^([varchar]+)\((\d+)\)$/).exec(column.COLUMN_TYPE.toLowerCase());
                                    let typeName = t[1];
                                    let typeLength = t[2];
                                    if(object[column.COLUMN_NAME].length > typeLength) {
                                        fn.faultValues.push({ column: column.COLUMN_NAME, issue: 'overflow'  })
                                    }
                                }
                                else if((/([blob]+)/).test(column.COLUMN_TYPE.toLowerCase())) {
                                    try {
                                        // try {
                                        //     if(!(/^([A-Za-z0-9+/]{4})*([A-Za-z0-9+/]{4}|[A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{2}==)$/).test(object[column.COLUMN_NAME])) { // if NOT base64 string
                                        //         fn.faultValues.push({ column: column.COLUMN_NAME, issue: 'not_base64_string'  });
                                        //         return;
                                        //     }
                                        // }
                                        // catch (exception) {
                                        //     return;
                                        // }

                                        // ******** OBS.: IF THE ABOVE TEST IS RUN, THE FILE WON'T RECORD CORRECTLY (DON'T KNOW WHY) ********


                                        object[column.COLUMN_NAME] = Buffer.from(object[column.COLUMN_NAME], 'base64'); // parse Buffer
                                        // var fs = require('fs');
                                        // var wstream = fs.createWriteStream('/rj/home/teste.pdf');
                                        // wstream.write(Buffer.from(object[column.COLUMN_NAME], 'base64'));
                                        // wstream.end();
                                        // object[column.COLUMN_NAME] = fs.readFileSync('/home/rj/teste.pdf');// parse Buffer
                                    }
                                    catch (exception) {
                                        console.log("BLOB handling exception: " + exception);
                                        delete object[column.COLUMN_NAME];
                                    }
                                }
                            }
                        }

                        if(parameters && parameters.where && parameters.where != 'undefined') {
                            for(let c in parameters.where) { // remove unexpected columns (weird syntax or not present)
                                var column = (/([0-9a-zA-Z_]*)( [0-9a-zA-Z_]*)?/).exec(c)[1];
                                if((!(/^[0-9a-zA-Z_]*$/).test(column)) || _.findIndex(tableMetadata, t => t.COLUMN_NAME == column) === -1) {
                                    delete parameters.where[c];
                                }
                            }

                            var whereColumns = Object.keys(parameters.where);

                            if(whereColumns.length) {
                                fn.where = { columns: [], values:[], sqlColumns:'' };
                                for (let i = 0; i < whereColumns.length; i++) {
                                    let column = (/([0-9a-zA-Z_]*)( [0-9a-zA-Z_]*)?/).exec(whereColumns[i])[1];
                                    let signal = parameters.where[whereColumns[i]] === null ?  ' IS ' : "="; // standard

                                    fn.where.columns.push(column);

                                    if((/^([0-9a-zA-Z_]*) (not|NOT)$/).test(whereColumns[i])) { signal = parameters.where[whereColumns[i]] === null ?  ' IS NOT ' : "!="; }
                                    if((/^([0-9a-zA-Z_]*) (like|LIKE)$/).test(whereColumns[i])) { signal = " LIKE "; }
                                    if((/^([0-9a-zA-Z_]*) (not like|NOT LIKE)$/).test(whereColumns[i])) { signal = " NOT LIKE "; }

                                    fn.where.sqlColumns += i > 0 ? " AND " : "";

                                    if(reference.base.name === 'mysql') fn.where.sqlColumns += column + signal + (parameters.where[whereColumns[i]] === null ? "NULL" : "?");
                                    if(reference.base.name === 'mssql') fn.where.sqlColumns += column + signal + (parameters.where[whereColumns[i]] === null ? "NULL" : ("@" + column));
                                }

                                fn.where.values = (() => {
                                    var values = [];
                                    for (let i = 0; i < whereColumns.length; i++) {
                                        if(parameters.where[whereColumns[i]] !== null) {
                                            values.push(parameters.where[whereColumns[i]]);
                                        }
                                    }
                                    return values;
                                })()
                            }
                        }
                    }

                    switch(reference.base.name) {
                        case 'mysql': {
                            return new Promise((resolve, reject) => {
                                var conn = reference.base.createConnection(reference.databaseOptions);
                                conn.query({
                                    sql: `SELECT COLUMN_NAME, COLUMN_TYPE, CHARACTER_MAXIMUM_LENGTH, COLUMN_KEY, EXTRA, IS_NULLABLE
                                    FROM information_schema.columns
                                    WHERE TABLE_SCHEMA=?
                                    AND TABLE_NAME=?`,
                                    timeout: reference.databaseOptions.timeout || 5000
                                }, [reference.databaseOptions.database, TABLE_NAME], (errorMD, resultsMD, fieldsMD) => { // information_schema - table columns info
                                    if(errorMD) { conn.end(); reject('(SQL Error) ' + errorMD); return; }
                                    parseObject(resultsMD);
                                    if(_.filter(fn.faultValues, f => f.issue !== 'required' ).length > 0) {
                                        conn.end();
                                        reject(fn.faultValues);
                                        return;
                                    }

                                    var checkInsertOrUpdate = new Promise((resolve, reject) => {
                                        var pkList = _.filter(resultsMD, r => r.COLUMN_KEY === 'PRI');
                                        var searchColumns = "";
                                        var searchValues = [];
                                        if(pkList.length) { // has primary keys
                                            for(let i in pkList) {
                                                if(_.findIndex(Object.keys(object), k => k === pkList[i].COLUMN_NAME) > -1) { // of sent data, check if primary key is set
                                                    searchColumns += (i > 0) ? " AND " : "";
                                                    searchColumns += pkList[i].COLUMN_NAME + "=?";
                                                    searchValues.push(object[pkList[i].COLUMN_NAME]);
                                                }
                                            }

                                            if(searchValues.length) { // if object has PK value set
                                                conn.query({
                                                    sql: `SELECT 1
                                                    FROM ` + TABLE_NAME + ` WHERE ` + searchColumns,
                                                    timeout: reference.databaseOptions.timeout || 5000 },
                                                searchValues,
                                                (error, results, fields) => {
                                                    if(error) { conn.end(); console.log(error); reject('(SQL Error) ' + error); return; }
                                                    if(results.length) { // if pk is found (update instead of insert)
                                                        fn.where = { sqlColumns: searchColumns, values: searchValues };
                                                        resolve('UPDATE');
                                                        return;
                                                    }
                                                    else { // registry with specified PK not found in database
                                                        if(fn.faultValues.length > 0) reject(fn.faultValues);
                                                        resolve('INSERT');
                                                        return;
                                                    }
                                                });
                                            }
                                            else if(fn.where && fn.where.values.length) { // PK not specified and 'where' is set in parameters
                                                resolve('UPDATE'); // ergo UPDATA [table] SET X WHERE Y
                                                return;
                                            }
                                            else {
                                                if(fn.faultValues.length > 0) reject(fn.faultValues);
                                                resolve('INSERT');
                                                return;
                                            }
                                        }
                                    })

                                    checkInsertOrUpdate.then(operation => {
                                        switch (operation) {
                                            case 'INSERT': {
                                                conn.query({
                                                    sql: "INSERT INTO " + TABLE_NAME + " SET ?",
                                                    timeout: reference.databaseOptions.timeout || 5000 },
                                                    object, (error, results, fields) => {
                                                    if(error) { conn.end(); reject('(SQL Error) ' + error); return; }
                                                    results.affectedId = results.insertId;

                                                    conn.end();
                                                    resolve(results);
                                                    return;
                                                });
                                                break;
                                            }
                                            case 'UPDATE': {
                                                fn.update = { sqlColumns: '', values: [] }
                                                var notPkList = _.filter(resultsMD, r => r.COLUMN_KEY !== 'PRI');
                                                for(let i in notPkList) {
                                                    if(typeof object[notPkList[i].COLUMN_NAME] != 'undefined') {
                                                        fn.update.sqlColumns += (i > 0 && fn.update.sqlColumns != '') ? ", " : "";
                                                        fn.update.sqlColumns += notPkList[i].COLUMN_NAME + "=?";
                                                        fn.update.values.push(object[notPkList[i].COLUMN_NAME]);
                                                    }
                                                }

                                                conn.query({
                                                    sql: "UPDATE " + TABLE_NAME + " SET " + fn.update.sqlColumns + " WHERE " + fn.where.sqlColumns,
                                                    timeout: reference.databaseOptions.timeout || 5000 },
                                                    [...fn.update.values, ...fn.where.values], (error, results, fields) => {
                                                    if(error) { conn.end(); reject('(SQL Error) ' + error); return; }
                                                    results.affectedId = object[_.filter(resultsMD, r => r.COLUMN_KEY === 'PRI')[0].COLUMN_NAME];

                                                    conn.end();
                                                    resolve(results);
                                                    return;
                                                });
                                                break;
                                            }
                                        }
                                    })
                                    .catch(error => {
                                        conn.end(); reject(error); return;
                                    })
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
                                    requestMetadata.input('TABLE_NAME', TABLE_NAME);
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
                                    requestMetadata.on('done', affected => {
                                        parseObject(rsMetadata.rows);
                                        if(_.filter(fn.faultValues, f => f.issue !== 'required' ).length > 0) {
                                            db.close();
                                            reject(fn.faultValues);
                                            return;
                                        }

                                        var checkInsertOrUpdate = new Promise((resolve, reject) => {
                                            var pkList = _.filter(rsMetadata.rows, r => r.COLUMN_KEY === 'PRI');
                                            var searchColumns = "";
                                            var searchValues = [];
                                            if(pkList.length) { // has primary keys
                                                for(let i in pkList) {
                                                    if(_.findIndex(Object.keys(object), k => k === pkList[i].COLUMN_NAME) > -1) { // of sent data, check if primary key is set
                                                        searchColumns += (i > 0) ? " AND " : "";
                                                        searchColumns += pkList[i].COLUMN_NAME + "=@" + pkList[i].COLUMN_NAME;
                                                        searchValues.push(object[pkList[i].COLUMN_NAME]);
                                                    }
                                                }

                                                if(searchValues.length) { // if object has PK value set
                                                    var requestSearch = db.request();
                                                    var rsSearch = { rows: [] }
                                                    for(let i in pkList) { requestSearch.input(pkList[i].COLUMN_NAME, object[pkList[i].COLUMN_NAME]); }
                                                    requestSearch.query(`SELECT 1 FROM ` + TABLE_NAME + ` WHERE ` + searchColumns);
                                                    requestSearch.on('error', error => { reject('(SQL Error) ' + error.stack);  });
                                                    requestSearch.on('row', row => { rsSearch.rows.push(row); });
                                                    requestSearch.on('done', affected => {
                                                        if(rsSearch.rows.length) { // if pk is found (update instead of insert)
                                                            fn.where = { sqlColumns: searchColumns, values: searchValues };
                                                            resolve('UPDATE');
                                                            return;
                                                        }
                                                        else {
                                                            if(fn.faultValues.length > 0) reject(fn.faultValues);
                                                            resolve('INSERT');
                                                            return;
                                                        }
                                                    })
                                                }
                                                else if(fn.where && fn.where.values.length) { // PK not specified and 'where' is set in parameters
                                                    resolve('UPDATE'); // ergo UPDATA [table] SET X WHERE Y
                                                    return;
                                                }
                                                else {
                                                    if(fn.faultValues.length > 0) reject(fn.faultValues);
                                                    resolve('INSERT');
                                                    return;
                                                }
                                            }
                                        })

                                        checkInsertOrUpdate.then(operation => {
                                            switch (operation) {
                                                case 'INSERT': {
                                                    var requestInsertColumns = "";
                                                    var requestInsertValues = "";
                                                    var requestExecute = db.request();
                                                    var rsExecute = { };
                                                    var objectKeys = Object.keys(object);
                                                    for(let i in objectKeys){
                                                        requestInsertColumns += (i > 0) ? "," : ""; requestInsertColumns += objectKeys[i];
                                                        requestInsertValues += (i > 0) ? "," : ""; requestInsertValues += "@" + objectKeys[i];
                                                        requestExecute.input(objectKeys[i], object[objectKeys[i]]);
                                                    }
                                                    requestExecute.query(`INSERT INTO ` + TABLE_NAME + ` (` + requestInsertColumns + `) VALUES (` + requestInsertValues + `); SELECT SCOPE_IDENTITY() AS insertId`);
                                                    requestExecute.on('error', error => { reject('(SQL Error) ' + error.stack);  });
                                                    requestExecute.on('row', row => { rsExecute.affectedId = row.insertId });
                                                    requestExecute.on('done', affected => {
                                                        db.close();
                                                        resolve(rsExecute);
                                                        return;
                                                    })
                                                    break;
                                                }
                                                case 'UPDATE': {
                                                    fn.update = { columns: '' }
                                                    var requestExecute = db.request();
                                                    var rsExecute = { };
                                                    var pkList = _.filter(rsMetadata.rows, r => r.COLUMN_KEY === 'PRI');
                                                    var notPkList = _.filter(rsMetadata.rows, r => r.COLUMN_KEY !== 'PRI');
                                                    for(let i in notPkList) {
                                                        if(typeof object[notPkList[i].COLUMN_NAME] != 'undefined') {
                                                            fn.update.columns += (i > 0 && fn.update.columns != '') ? "," : "";
                                                            fn.update.columns += notPkList[i].COLUMN_NAME + "=@" + notPkList[i].COLUMN_NAME;
                                                            requestExecute.input(notPkList[i].COLUMN_NAME, object[notPkList[i].COLUMN_NAME]);
                                                        }
                                                    }
                                                    for(let i in pkList) { requestExecute.input(pkList[i].COLUMN_NAME, object[pkList[i].COLUMN_NAME]); }
                                                    for(let i in fn.where.columns){
                                                        requestExecute.input(fn.where.columns[i], fn.where.values[i])
                                                    }
                                                    requestExecute.query(`UPDATE ` + TABLE_NAME + ` SET ` + fn.update.columns + ` OUTPUT Inserted.` + pkList[0].COLUMN_NAME + ` AS updateId WHERE ` + fn.where.sqlColumns);
                                                    requestExecute.on('error', error => { reject('(SQL Error) ' + error.stack);  });
                                                    requestExecute.on('row', row => { rsExecute.affectedId = row.updateId });
                                                    requestExecute.on('done', affected => {
                                                        db.close();
                                                        resolve(rsExecute);
                                                        return;
                                                    })
                                                    break;
                                                }
                                            }
                                        })
                                        .catch(error => {
                                            db.close(); reject(error); return;
                                        })


                                        // var request = db.request();
                                        // var rs = { rows: [] }
                                        //
                                        // if(fn.where.columns) {
                                        //     for(let i in fn.where.columns) {
                                        //         request.input(fn.where.columns[i], fn.where.values[i]);
                                        //     }
                                        // }
                                        // request.query(fn.sql);
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
        },
        query: SQL => {
            var fn = { };
            return {
                execute: parameters => {
                    fn.values = [];
                    var sqlSets = SQL.match(/\{(\w+)\}/g);
                    for(let i in sqlSets) sqlSets[i] = (/\{([\d\w_]*)\}/).exec(sqlSets[i])[1]; // remove { }
                    if(parameters && sqlSets) {
                        for(var p in parameters){ if(sqlSets.indexOf(p) === -1){ delete parameters[p];} else { fn.values.push(parameters[p]); } }// delete all parameters not present in sql
                    }

                    switch(reference.base.name) {
                        case 'mysql': {
                            return new Promise((resolve, reject) => {
                                if(parameters && sqlSets && sqlSets.length > Object.keys(parameters).length) { reject("Missing parameters"); return;}
                                if(parameters) for(var p in parameters) SQL = SQL.replace(new RegExp("{" + p + "}", "g"), '?');

                                var conn = reference.base.createConnection(reference.databaseOptions);

                                // console.log(SQL);
                                // console.log(parameters);

                                conn.query({ sql: SQL, timeout: 4000 }, fn.values, (error, results, fields) => {
                                    if(error) { conn.end(); reject(Error('Error on connection: ' + error)); return; }
                                    conn.end();
                                    resolve(results);
                                    return;
                                });
                            })
                            break;
                        }
                        case 'mssql': {
                            return new Promise((resolve, reject) => {
                                if(parameters && sqlSets.length > Object.keys(parameters).length) { reject("Missing parameters"); return;}
                                if(parameters) for(var p in parameters) SQL = SQL.replace(new RegExp("{" + p + "}", "g"), '@'+p);

                                var db = new reference.base.Connection(reference.databaseOptions);

                                db.connect(error => {
                                    if(error) { db.close(); reject('Error on connection: ' + error.stack); return; }
                                    var request = db.request();
                                    var rs = { rows: [] }
                                    for(var p in parameters ) request.input(p, parameters[p]);
                                    request.query(SQL);
                                    // console.log(SQL);
                                    request.on('error', error => { reject('(SQL Error) ' + error.stack);  });
                                    // request.on('recordset', columns => { rs.columns = columns;  });
                                    request.on('row', row => { rs.rows.push(row); });
                                    request.on('done', affected => {
                                        rs.affected = affected;
                                        db.close();
                                        resolve(rs);
                                        return;
                                    });
                                })
                            })
                            break;
                        }
                    }
                }
            }
        }
    }
}
