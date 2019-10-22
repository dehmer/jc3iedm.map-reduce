const R = require('ramda')
const { kv } = require('./combinators')
const ent_subt = require('../meta/ent_subt.json')

/**
 * Collect all subtype/child records in one object
 * per primary key (along with suptype/parent record).
 */
module.exports = async streams => {
  const { db, stream, map, curriedReduce, object } = streams
  const split_key = data => [...data.key.split(':'), data.value]
  const pa_kv = ([ent_id, pk, value]) => [pk, kv(ent_id, value)]
  const pa_rec = R.compose(pa_kv, split_key)
  const mat_pa_recs = R.compose(object, map(pa_rec), stream)
  const insert_ch_rec = pa_recs => ([ent_id, pk, value]) => (pa_recs[pk][ent_id] = value)
  const merge_ch_rec = pa_recs => R.compose(R.always(pa_recs), insert_ch_rec(pa_recs), split_key)
  const merge_ch_recs = pa_recs => R.compose(curriedReduce(merge_ch_rec, pa_recs), stream)

  return Object.entries(ent_subt).map(async ([pa, chs]) => {
    const pa_recs = await mat_pa_recs(pa)
    await Promise.all(chs.map(merge_ch_recs(pa_recs)))
    const op = ([key, value]) => ({ type: 'put', key: `TARGET:${pa}:${key}`, value})
    const ops = Object.entries(pa_recs).map(op)
    return db.batch(ops)
  })
}
