const express = require('express');
const app = express();
const ndbc = require('./lib/ndbc');
const sumTryCatch = require('./sum-try-catch');
const mssql = require('mssql');
const { performance } = require('perf_hooks');

const db = {
    mysql: {
        north: new ndbc.mysql.Reference({ host:'127.0.0.1', user:'root', password:'root', database:'north', timezone: 0300 }),
        aquadoc: new ndbc.mysql.Reference({ host:'127.0.0.1', user:'root', password:'root', database:'db_aquadoc', timezone: 0300 })
    },
    mssql: {
        north: new ndbc.mssql.Reference({ user: 'SA', password: 'Fly1507!',server: '127.0.0.1', database: 'north', stream:true, connectionTimeout:30000, requestTimeout:30000, options: { encrypt: false } }),
        first: new ndbc.mssql.Reference({ user: 'SA', password: 'Aqua12!',server: '172.16.1.2', database: 'first', stream:true, connectionTimeout:30000, requestTimeout:30000, options: { encrypt: false } })
    }
}

const run = (async () => {
    const people = await db.mysql.aquadoc.table('pessoas').getData({ where:{ id:1 } });
    console.log(people);
    
})()

// const t0 = performance.now();
// db.mssql.north.table('people').getData().then(people => {
//     const t1 = performance.now();
//     console.log(people);
//     console.log("Call to doSomething took " + (t1 - t0) + " milliseconds.");    
// })

// db.mssql.north.table('people').setData({ name:`I'm Thirteen "GREAT!` }).then(response => {
//     db.mssql.north.table('people').getData().then(people => {
//         console.log(people);
//     })
    
// })
// .catch(error => {
//     console.log(error);    
// })


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
    const data = await sumTryCatch(db.mssql.north.table('people').getData());

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

// app.listen(4000);

