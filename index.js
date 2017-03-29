var express = require('express');
var app = express();
var ndbc = require('./lib/ndbc');

var db = {
    mysql: {
        north: new ndbc.mysql.Reference({ host:'127.0.0.1', user:'root', password:'root', database:'north', timezone: 0300 }),
        aquadoc: new ndbc.mysql.Reference({ host:'127.0.0.1', user:'root', password:'root', database:'db_aquadoc', timezone: 0300 })
    },
    mssql: {
        north: new ndbc.mssql.Reference({ user: 'SA', password: 'Fly11031507',server: '127.0.0.1', database: 'north', stream:true, connectionTimeout:30000, requestTimeout:30000 }),
        first: new ndbc.mssql.Reference({ user: 'SA', password: 'Aqua12!',server: '172.16.100.100', database: 'first', stream:true, connectionTimeout:30000, requestTimeout:30000 })
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
    // ** MySQL JOIN EXAMPLE **
    // db.mysql.aquadoc.table('pessoas')
    // .join({
    //     type:'inner',
    //     table: {
    //         name:'pessoas',
    //         alias:'fornecedor',
    //         columns:['id','nome']
    //     },
    //         on:[{ foreignColumn:'representantecomercial', column:'id' },{ foreignColumn:'nomecomercial', column:'fornecedor' }]
    //     })
    // .join({ type:'inner', table: { name:'pessoas', alias:'representante', columns:['id','nome']}, on:[{ foreignColumn:'representantecomercial', column:'id' }] })
    // .getData({ where:{ id:1 } }).then(pessoas => {
    //     console.log(pessoas);
    // })

    // ** MySQL JOIN EXAMPLE (realistic)**
    db.mysql.aquadoc.table('pessoas')
    .join({ table:{ name:'pessoas', alias:'representante', columns:['nome']}, on:[{ foreignColumn:'representantecomercial', column:'id' }] })
    .getData({
        columns:['id','nome'],
        where:{ id:1 }
    }).then(pessoas => {
        res.json(pessoas);
    })

    // ** SQL SERVER JOIN EXAMPLE (realistic)**
    // db.mssql.north.table('todos')
    // .join({ table:{ name:'people', alias:'responsible', columns:['name']}, on:[{ foreignColumn:'responsible', column:'id' }] })
    // .getData({ where:{ 'id not': 0 }}).then(people => {
    //     res.json(people);
    // })

    // db.mysql.aquadoc.table('pessoas').getData({ where:{ id:1 } }).then(pessoas => {
    //     res.json(pessoas);
    // })

    // db.mssql.first.table('SA1').getData({ where:{ A1_NOME:'RENNE JASKONIS' } }).then(people => {
    //     console.log(people);
    //     res.send('people');
    // }).catch(err => {
    //     console.log(err)
    //     res.status(500).json('error');
    // })

    // db.mysql.north.table('people').setData({ id:5, name:'Renne J', birthdate:'1981-11-20', money:0 }).then(response => {
    //     console.log(response);
    //     res.send(':)');
    // }).catch(err => {
    //     console.log(err);
    //     res.status(500).json('error');
    // })

    // db.mysql.north.table('people').setData({ money: null }, { where:{ 'birthdate not':null} }).then(response => {
    //     console.log(response);
    //     res.send(':)');
    // }).catch(err => {
    //     console.log(err);
    //     res.status(500).json('error');
    // })

    // PROTOTYPE
    // db.mysql.north.query("select p.*, 1 as 'p.teste' from people p").execute({ description: '%T%'}).then(list => {
    //     res.send(list);
    // }).catch(err => {
    //     console.log(err)
    //     res.status(500).json('error');
    // })
})

app.listen(3000);
