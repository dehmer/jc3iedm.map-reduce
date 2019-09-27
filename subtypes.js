const subtypes = require('./mird/ent_subt.json')

const K = v => fn => { fn(v); return v }
const kv = (k, v) => K({})(o => o[k] = v)

const inheritance = streams => async entityId => {
  const { db, stream, map, reduce, list, object } = streams

  const root = data => {
    const [entityId, pk] = data.key.split(':')
    return [pk, kv(entityId, data.value)]
  }

  const roots = await object(map(root)(stream(entityId)))

  // Attach child record to root with same primary key:
  const attach = (acc, data) => {
    const [entityId, pk] = data.key.split(':')
    acc[pk][entityId] = data.value
    return acc
  }

  // Augment roots with subtype records:
  const ps = Object.values(subtypes[entityId])
    .map(entityId => stream(entityId))
    .map(reduce(attach, roots))

  await Promise.all(ps)
  const op = ([key, value]) => ({ type: 'put', key: `${entityId}:${key}`, value})
  const ops = Object.entries(roots).map(op)
  return db.batch(ops)
}

const reduce = streams => async () =>
  Promise.all(Object.keys(subtypes).map(entityId => inheritance(streams)(entityId)))

module.exports = streams => ({
  reduce: reduce(streams)
})