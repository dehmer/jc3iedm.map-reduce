const { Transform, PassThrough, Writable } = require('stream')

const K = v => fn => { fn(v); return v }

const map = fn => stream => stream.pipe(new Transform({
  objectMode: true,
  transform (chunk, _, next) {
    this.push(fn(chunk))
    next()
  }
}))

const filter = fn => stream => stream.pipe(new Transform({
  objectMode: true,
  transform (chunk, _, next) {
    if (fn(chunk)) this.push(chunk)
    next()
  }
}))

const distinctOn = (on, order) => stream => {
  const writable = new Transform({ objectMode: true })
  const push = writable.push.bind(writable)
  const chunks = []

  const pop = () => {
    chunks.sort(order) // in-place
    push(chunks[0])
    chunks.length = 0
  }

  writable._transform = function (chunk, _, next) {
    const key = on(chunk)
    if (this.key && this.key !== key) pop()
    this.key = key
    chunks.push(chunk)
    next()
  }

  writable._final = function (next) {
    pop()
    next()
  }

  return stream.pipe(writable)
}

const partition = p => stream => {
  const a = new PassThrough({ objectMode: true })
  const b = new PassThrough({ objectMode: true })

  const writable = new Writable({
    objectMode: true,
    write (chunk, _, next) {
      if (p(chunk)) a.write(chunk)
      else b.write(chunk)
      next()
    },
    final (next) {
      a.end()
      b.end()
      next()
    }
  })

  stream.pipe(writable)
  return [a, b]
}

const reduce = (fn, initial) => stream => new Promise(resolve => {
  let acc = initial
  stream
    .on('data', data => (acc = fn(acc, data)))
    .on('end', () => resolve(acc))
})

const curriedReduce = (fn, initial) => stream => new Promise(resolve => {
  let acc = initial
  stream
    .on('data', data => (acc = fn(acc)(data)))
    .on('end', () => resolve(acc))
})

const object = stream => reduce((acc, [key, value]) => K(acc)(acc => acc[key] = value), {})(stream)
const list = stream => reduce((acc, o) => K(acc)(acc => acc.push(o)), [])(stream)

const put = db => stream => new Promise(resolve => {
  const batchSize = 500
  const ops = []
  const writeBatch = next => () => db.batch(ops, err => (ops.length = 0, next(err)))

  const writable =new Writable({
    objectMode: true,
    write (chunk, _, next) {
      ops.push({ type: 'put', ...chunk })
      ;(ops.length === batchSize ? writeBatch(next) : next)()
    },
    final (next) {
      writeBatch(next)()
      resolve()
    }
  })

  stream.pipe(writable)
})

const options = partition => ({ gt: `${partition}`, lt: `${partition}\xff`} )
const stream = db => partition => db.createReadStream(partition ? options(partition) : {})

module.exports = db => ({
  db,
  map,
  filter,
  distinctOn,
  partition,
  reduce,
  curriedReduce,
  object,
  list,
  put: put(db),
  stream: stream(db)
})