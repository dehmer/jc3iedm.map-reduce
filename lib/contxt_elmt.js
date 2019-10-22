const R = require('ramda')
const { K } = require('../lib/combinators')
const ent_id = require('../meta/ent_id')

/**
 * Filter context elements: most recent additions.
 */
module.exports = async streams => {
  const { db, stream, map, filter, list } = streams
  const ces = data => [data.key.split(':')[1].substring(0, 41), ...data.value]
  const mat_ces_recs = R.compose(list, map(ces), stream)
  const ces_recs = await mat_ces_recs(ent_id.CONTXT_ELMT_STAT)
  const group_ce = R.groupBy(ces_rec => ces_rec[0])
  const sort_effctv_dttm = R.sortBy(ces_rec => ces_rec[2])
  const additions = Object.entries(group_ce(ces_recs))
    .map(([pk, stats]) => [pk, sort_effctv_dttm(stats).reverse()[0][1]])
    .reduce((acc, [pk, ces_rec]) => K(acc)(acc => acc[pk] = ces_rec), {})

  const is_additn = ce_rec => additions[ce_rec.key.split(':')[1]] === 'ADDITN'
  const mat_ce_recs = R.compose(list, filter(is_additn), stream)
  const ce_recs = await mat_ce_recs(ent_id.CONTXT_ELMT)
  const op = ({key, value}) => ({ type: 'put', key: `TARGET:${key}`, value})
  const ops = ce_recs.map(op)
  return db.batch(ops)
}
