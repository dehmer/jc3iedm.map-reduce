#!/usr/bin/env node
const level = require('level')
const db = level('db', { valueEncoding: 'json' })
const { stream } = require('./lib/streams')(db)
const ent_id = require('./meta/ent_id')

let count = 0
const partition = `P01:10000137`
stream(partition)
  .on('data', () => count += 1)
  .on('end', () => console.log(partition, count))
