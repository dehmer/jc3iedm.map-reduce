#!/usr/bin/env node
const level = require('level')
const db = level('db', { valueEncoding: 'json' })
const { stream } = require('./lib/streams')(db)
const ent_id = require('./meta/ent_id')

stream(`P02:${ent_id.CONTXT_OI_ASSOC}`).on('data', data => console.log(data.key, '=', data.value))
