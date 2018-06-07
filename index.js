<<<<<<< HEAD
module.exports = require('./lib/ndbc');
=======
var express = require('express');
var app = express();
var ndbc = require('./lib/ndbc');

var db = {
    mysql: {
        north: new ndbc.mysql.Reference({ host:'127.0.0.1', user:'root', password:'root', database:'north', timezone: 0300 }),
        aquadoc: new ndbc.mysql.Reference({ host:'127.0.0.1', user:'root', password:'root', database:'db_aquadoc', timezone: 0300 })
    },
    mssql: {
        north: new ndbc.mssql.Reference({ user: 'SA', password: 'Fly1507!',server: '127.0.0.1', database: 'north', stream:true, connectionTimeout:30000, requestTimeout:30000 }),
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

app.get('/', async (req, res) => {
    // ** MySQL JOIN EXAMPLE **
    // const acompanhamentosproducao = await db.mysql.aquadoc.table('acompanhamentosproducao').getData({ columns:['id', 'produto', 'dataregistro'], datetimeFormat:'DD/MM/YYYY' });

    // console.log(acompanhamentosproducao);
    // return res.json(acompanhamentosproducao);

    // ** MySQL JOIN EXAMPLE (realistic)**
    // db.mysql.aquadoc.table('pessoas')
    // .getData({
    //     columns:['id','nome'],
    //     where:{ id:1 },
    //     joins: [
    //         { table:{ name:'pessoas', alias:'representante', columns:['nome', 'nomefantasia']}, on:[{ foreignColumn:'representantecomercial', column:'id' }] }
    //     ]
    // }).then(pessoas => {
    //     res.json(pessoas);
    // })

    // ** SQL SERVER JOIN EXAMPLE (realistic)**
    const data = await db.mssql.north.table('todos').getData({ 
        where:{ 'id not': 0 }, 
        datetimeFormat:'DD/MMM YYYY',
        joins: [
            { table:{ name:'people', alias:'person' }, on:[{ foreignColumn:'responsible', column:'id' }] }
        ]
    });

    return res.json(data)

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

app.listen(4000);
>>>>>>> d16f03616d638d4f2688286aec2b51d746518a69
