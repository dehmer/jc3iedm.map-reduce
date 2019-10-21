const {Transform} = require('stream')
const {ByteBuffer, UnderflowError} = require('./bytebuffer')
const fieldCount = require('../meta/pk_field_count.json')

/**
 * Decoder with internal accumulator.
 * @param {function} fn decoder function
 */
const accumulatingDecoder = fn => new Transform({
  objectMode: true,
  transform (chunk, _, next) {
    this.fn = this.fn || fn
    this.acc = (this.acc || ByteBuffer.empty()).append(chunk).shrink()

    while(this.acc.remaining()) {
      try {
        this.fn = this.fn(this.acc.mark(), chunk => this.push(chunk))
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


/**
 *
 */
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
    const value = record.substring(fieldCount[entityId] * 21).split('|')
    value.length = value.length - 2
    push([entityId, key, value])
    return buffer.fixed(1) === '}' ? decodeEntityId : decodeRecord
  }

  return accumulatingDecoder(decodeX01)
}

module.exports = x01Decoder