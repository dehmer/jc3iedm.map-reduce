const { Transform } = require('stream')

const K = v => fn => { fn(v); return v }

const map = fn => stream => stream.pipe(new Transform({
  objectMode: true,
  transform (chunk, _, next) {
    this.push(fn(chunk))
    next()
  }
}))

const reduce = (fn, initial) => stream => new Promise(resolve => {
  let acc = initial
  stream
    .on('data', data => (acc = fn(acc, data)))
    .on('end', () => resolve(acc))
})

const object = stream => reduce((acc, [key, value]) => K(acc)(acc => acc[key] = value), {})(stream)
const list = stream => reduce((acc, o) => K(acc)(acc => acc.push(o)), [])(stream)
const options = entityId => ({ gt: `${entityId}`, lt: `${entityId}\xff`} )
const stream = db => entityId => db.createReadStream(entityId ? options(entityId) : {})

module.exports = db => ({
  db,
  map,
  reduce,
  object,
  list,
  stream: stream(db)
})