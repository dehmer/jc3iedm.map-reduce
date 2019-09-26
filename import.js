#!/usr/bin/env node
const fs = require('fs')
const {Transform, Writable} = require('stream')
const level = require('level')
const now = require('nano-time')
const {ByteBuffer, UnderflowError} = require('./lib/bytebuffer')
const argv = require('minimist')(process.argv.slice(2), {boolean: 't', string: 'd'})
const input = argv._.length ? fs.createReadStream(argv._[0]) : process.stdin
const db = level('db', { valueEncoding: 'json' })
const fieldCount = require('./mird/ent_pk_fields.json')

// Decoder with internal accumulator.
const accDecoder = fn => new Transform({
  objectMode: true,
  transform (chunk, _, next) {
    this.fn = this.fn || fn
    this.acc = this.acc || ByteBuffer.empty()
    if(Buffer.isBuffer(chunk)) this.acc.append(chunk).shrink()
    else return this.push(chunk), next()

    while(this.acc.remaining()) {
      try {
        this.acc.mark()
        this.fn = this.fn(this.acc, chunk => this.push(chunk))
      }
      catch(err) {
        if(err instanceof UnderflowError) return this.acc.reset(), next()
        // see: https://nodejs.org/api/stream.html#stream_errors_while_writing
        else return next(err)
      }
    }

    next()
  }
})

const x01Decoder = () => {
  let entityId

  const decodeX01 = buffer => {
    const X01 = buffer.fixed(3)
    if(X01 !== 'X01') throw new Error(`invalid format: ${X01}`)
    return decodeEntityId
  }

  const decodeEntityId = buffer => {
    entityId = buffer.fixed(8)
    buffer.skip(1) // '{'
    return buffer.fixed(1) === '{' ? decodeRecord : decodeEntityId
  }

  const decodeRecord = (buffer, push) => {
    const record = buffer.delimited('}', '`')
    const key = record.substring(0, fieldCount[entityId] * 21 - 1)
    const value = record.substring(fieldCount[entityId] * 21)
    // TODO: value array; strip creator_id, update_seqnr_ord
    push([entityId, key, value])
    return buffer.fixed(1) === '}' ? decodeEntityId : decodeRecord
  }

  return accDecoder(decodeX01)
}

const batchWriter = () => {
  let count = 0
  let ops = []

  const writeBatch = next => {
    db.batch(ops, err => {
      if (err) return next(err)
      count = 0
      ops.length = 0
      next()
    })
  }

  return new Writable({
    objectMode: true,
    write (chunk, _, next) {
      count += 1
      ops.push({ type: 'put', key: `${chunk[0]}:${chunk[1]}`, value: chunk[2]})
      if (count === 500) writeBatch(next)
      else next()
    },

    final (next) {
      writeBatch(next)
    }
  })
}

input.pipe(x01Decoder()).pipe(batchWriter())