#!/usr/bin/env node
const level = require('level')
const db = level('db', { valueEncoding: 'json' })
const { stream } = require('./lib/streams')(db)

const ops = []
stream(`P01:`)
  .on('data', data => ops.push({type: 'del', key: data.key}))
  .on('end', () => db.batch(ops))
