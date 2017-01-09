module.exports = base => {
    return class Reference {
        constructor (databaseOptions) {
            this.databaseOptions = databaseOptions;
            this.baseName = base;
            this.base = require(base);
            this.dbfn = require('./dbfn')(this)

            this.table.bind(this);
        }

        table(name) {
            if(!(/^[0-9a-zA-Z_]*$/).test(name)) {
                throw "Invalid table name";
            }

            this.fn = { table:{ name }};
            return Object.assign({ }, this.dbfn.table);
        }
    }
}
