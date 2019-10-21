const R = require('ramda')
const { K } = require('../lib/combinators')
const ent_id = require('../meta/ent_id')

/**
 * object items included in context.
 */
module.exports = async streams => {
  const { db, stream, map, filter, list } = streams

  const status = data => {
    // pk: contxt_id, obj_item_id
    const pk = data.key.split(':')[1].substring(0, 41)
    return [pk, ...data.value]
  }

  const stats = await list(map(status)(stream(ent_id.CONTXT_OI_ASSOC_STAT)))
  const group = R.groupBy(stat => stat[0])
  const sort = R.sortBy(stat => stat[2])

  const starts = Object.entries(group(stats))
    // TODO: CONTXT_OI_ASSOC_STAT.establishing organization
    .map(([pk, stats]) => [pk, sort(stats).reverse()[0][1]])
    .reduce((acc, [pk, stat]) => K(acc)(acc => acc[pk] = stat), {})

  const include = data => data.value[0] === 'INCLDE'
  const start = data => starts[data.key.split(':')[1]] === 'START'
  const relevant = data => include(data) && start(data)
  const items = await list(filter(relevant)(stream(ent_id.CONTXT_OI_ASSOC)))
  const op = ({key}) => ({ type: 'put', key: `TARGET:${key}`, value: { /* empty */ }})
  const ops = items.map(op)
  return db.batch(ops)
}
