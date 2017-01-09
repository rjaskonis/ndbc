module.exports = {
    mysql: {
        Reference: require('./reference')('mysql')
    },
    mssql: {
        Reference: require('./reference')('mssql')
    }
}
