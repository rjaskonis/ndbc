var express = require('express');
var app = express();
var ndbc = require('./lib/ndbc');

var db = {
    mysql: {
        north: new ndbc.mysql.Reference({ host:'127.0.0.1', user:'root', password:'root', database:'north', timezone: 0300 }),
        aquadoc: new ndbc.mysql.Reference({ host:'aquanalyze.no-ip.biz', user:'root', password:'Uxroot', database:'db_aquadoc', timezone: 0300 })
    },
    mssql: {
        north: new ndbc.mssql.Reference({ user: 'SA', password: 'Fly11031507',server: '127.0.0.1', database: 'north', stream:true, connectionTimeout:30000, requestTimeout:30000 }),
        first: new ndbc.mssql.Reference({ user: 'SA', password: 'Aqua12!',server: 'aquanalyze.no-ip.biz', database: 'first', stream:true, connectionTimeout:30000, requestTimeout:30000 })
    }
}


// db.north.table('todos').getData().then(todos => {
//     console.log(todos);
// }).catch(err => console.log(err))
//
// db.aquadoc.table('tarefas').getData().then(tarefas => {
//     console.log(tarefas);
// }).catch(err => console.log(err))
//
// db.first.table('SG1').getData().then(data => {
//     console.log(data);
// }).catch(err => console.log(err))
//
// console.log('** NDBC **');

app.get('/', (req, res) => {
    // db.mssql.north.table('todos').getData({ where: { 'description like': 'F%' }}).then(people => {
    //     res.send(people);
    // }).catch(err => {
    //     console.log(err)
    //     res.status(500).json('error');
    // })

    db.mssql.north.table('people').setData({ id:1,  }).then(response => {
        console.log(response);
        res.send(':)');
    }).catch(err => {
        console.log(err);
        res.status(500).json('error');
    })

    // PROTOTYPE
    // db.mysql.north.storedQuery('my-query-name').getData({ parameters: [] }).then(list => {
    //     res.send(list);
    // }).catch(err => {
    //     console.log(err)
    //     res.status(500).json('error');
    // })
})

app.listen(3000);
