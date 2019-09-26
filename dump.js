#!/usr/bin/env node
const level = require('level')
const db = level('db', { valueEncoding: 'json' })
const { stream } = require('./lib/streams')(db)

const CONTXT_ELMT = 10000168
const CONTXT_ELMT_STAT = 10000298
const LOC = '10000063'
const OBJ_ITEM = '10000076'
const OBJ_TYPE = 10000082
const RPTD = 10000157

const options = {}
stream(RPTD).on('data', data => console.log(data.key, '=', data.value))
