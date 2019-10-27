#!/usr/bin/env node
const fs = require('fs')
const {Writable} = require('stream')
const level = require('level')
const argv = require('minimist')(process.argv.slice(2), {boolean: 't', string: 'd'})
const x01Decoder = require('./lib/decoder')
const input = argv._.length ? fs.createReadStream(argv._[0]) : process.stdin
const db = level('db', { valueEncoding: 'json' })

const writer = (db, batchSize = 500) => {
  const ops = []
  const writeBatch = next => () => db.batch(ops, err => (ops.length = 0, next(err)))
  return new Writable({
    objectMode: true,
    write (chunk, _, next) {
      ops.push({ type: 'put', key: `X01:${chunk[0]}:${chunk[1]}`, value: chunk[2] })
      ;(ops.length === batchSize ? writeBatch(next) : next)()
    },
    final (next) {
      writeBatch(next)()
    }
  })
}

input.pipe(x01Decoder()).pipe(writer(db))