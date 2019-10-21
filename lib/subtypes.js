const { kv } = require('./combinators')
const ent_subt = require('../meta/ent_subt.json')

/**
 * Collect all subtype records in one object
 * per primary key (along with suptype record).
 */
module.exports = async streams => {
  const { db, stream, map, reduce, object } = streams

  // suptype :: key/value -> {pk -> {ent_id -> values}}
  const sup = data => {
    const [ent_id, pk] = data.key.split(':')
    return [pk, kv(ent_id, data.value)]
  }

  const load_sup = ent_id => object(map(sup)(stream(ent_id)))

  const append_sub = (sups, sub) => {
    const [ent_id, pk] = sub.key.split(':')
    sups[pk][ent_id] = sub.value
    return sups
  }

  return Object.entries(ent_subt).map(async ([sup, subs]) => {
    const sups = await load_sup(sup)
    await Promise.all(subs.map(sub => reduce(append_sub, sups)(stream(sub))))
    const op = ([key, value]) => ({ type: 'put', key: `TARGET:${sup}:${key}`, value})
    const ops = Object.entries(sups).map(op)
    return db.batch(ops)
  })
}
