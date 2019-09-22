#!/usr/bin/env node
const level = require('level')
const db = level('db', { valueEncoding: 'json' })

// const options = {}
const options = { gt: '10000063', lt: '10000063\xff' }
db.createReadStream(options).on('data', data => console.log(data.key, '=', data.value))
