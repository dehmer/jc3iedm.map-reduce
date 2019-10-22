const R = require('ramda')
const { K } = require('../lib/combinators')
const ent_id = require('../meta/ent_id')

/**
 * object items included in context.
 */
module.exports = async streams => {
  const { db, stream, map, filter, list } = streams
  const coias = data => [data.key.split(':')[1].substring(0, 41), ...data.value]
  const mat_coias_recs = R.compose(list, map(coias), stream)
  const coias_recs = await mat_coias_recs((ent_id.CONTXT_OI_ASSOC_STAT))
  const group_coia = R.groupBy(coias_rec => coias_rec[0])
  const sort_effctv_dttm = R.sortBy(coias_rec => coias_rec[2])
  const starts = Object.entries(group_coia(coias_recs))
    .map(([pk, stats]) => [pk, sort_effctv_dttm(stats).reverse()[0][1]])
    .reduce((acc, [pk, stat]) => K(acc)(acc => acc[pk] = stat), {})

  const is_inclde = data => data.value[0] === 'INCLDE'
  const is_start = data => starts[data.key.split(':')[1]] === 'START'
  const accept = data => is_inclde(data) && is_start(data)
  const mat_coia_recs = R.compose(list, filter(accept), stream)
  const coia_recs = await mat_coia_recs(ent_id.CONTXT_OI_ASSOC)
  const op = ({key}) => ({ type: 'put', key: `TARGET:${key}`, value: { /* empty */ }})
  const ops = coia_recs.map(op)
  return db.batch(ops)
}
