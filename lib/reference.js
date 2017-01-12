module.exports = baseName => {
    return class Reference {
        constructor (databaseOptions, engage) {
            this.databaseOptions = databaseOptions;
            this.base = require(baseName);
            this.base.name = baseName;
            this.dbfn = require('./dbfn')(this);

            this.table.bind(this);
            Object.assign(this, engage || {});
        }

        table(name) {
            if(!(/^[0-9a-zA-Z_]*$/).test(name)) {
                throw "Invalid table name";
            }

            return this.dbfn.table(name);
        }

        query(sql) {
            return this.dbfn.query(sql)
        }
    }
}
