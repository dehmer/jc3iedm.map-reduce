const R = require('ramda')
const { K } = require('../lib/combinators')
const ent_id = require('../meta/ent_id')

/**
 * Filter context elements: most recent additions.
 */
module.exports = async streams => {
  const { db, stream, map, filter, list } = streams

  const status = data => {
    // pk: contxt_id, contxt_elmt_ix
    const pk = data.key.split(':')[1].substring(0, 41)
    return [pk, ...data.value]
  }

  const stats = await list(map(status)(stream(ent_id.CONTXT_ELMT_STAT)))
  const group = R.groupBy(stat => stat[0])
  const sort = R.sortBy(stat => stat[2])

  const additions = Object.entries(group(stats))
    // TODO: CONTXT_ELMT_STAT.establishing organization
    .map(([pk, stats]) => [pk, sort(stats).reverse()[0][1]])
    .reduce((acc, [pk, stat]) => K(acc)(acc => acc[pk] = stat), {})

  const addition = data => additions[data.key.split(':')[1]] === 'ADDITN'
  const elmts = await list(filter(addition)(stream(ent_id.CONTXT_ELMT)))
  const op = ({key, value}) => ({ type: 'put', key: `TARGET:${key}`, value})
  const ops = elmts.map(op)
  return db.batch(ops)
}
