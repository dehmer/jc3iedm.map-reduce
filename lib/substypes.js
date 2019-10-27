const R = require('ramda')
const { kv } = require('./combinators')
const ent_subt = require('../meta/ent_subt.json')

module.exports = async streams => {
  const { db, stream, map, curriedReduce, object } = streams
  const split_key = data => [...data.key.split(':').slice(1), data.value]
  const pa_kv = ([ent_id, pk, value]) => [pk, kv(ent_id, value)]
  const pa_rec = map(R.compose(pa_kv, split_key))
  const insert_ch_rec = pa_recs => ([ent_id, pk, value]) => (pa_recs[pk][ent_id] = value)
  const merge_ch_rec = pa_recs => R.compose(R.always(pa_recs), insert_ch_rec(pa_recs), split_key)
  const merge_ch_recs = pa_recs => R.compose(curriedReduce(merge_ch_rec, pa_recs), stream)

  return Object.entries(ent_subt).map(async ([pa, chs]) => {
    const pa_recs = await R.compose(object, pa_rec, stream)(`X01:${pa}`)
    await Promise.all(chs.map(ch => merge_ch_recs(pa_recs)(`X01:${ch}`)))
    const op = ([key, value]) => ({ type: 'put', key: `P01:${pa}:${key}`, value})
    const ops = Object.entries(pa_recs).map(op)
    return db.batch(ops)
  })
}
