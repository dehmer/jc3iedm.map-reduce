const R = require('ramda')

const ACT_CONTXT = '10000005'
const ACT_CONTXT_STAT = '10000295'
const CONTXT_OI_ASSOC = '10000137'
const CONTXT_OI_ASSOC_STAT = '10000299'

const K = v => fn => { fn(v); return v }

const contxt_oi_assoc = streams => async () => {
  const { db, stream, reduce, filter, map } = streams
  const include = data => data.value[0] === 'INCLDE'
  const pk = data => data.key.split(':')[1]
  const item = (acc, pk) => K(acc)(acc => acc[pk] = [])
  const items = await R.compose(reduce(item, {}), map(pk), filter(include))(stream(CONTXT_OI_ASSOC))

  await reduce((acc, data) => {
    const pk = data.key.split(':')[1].substring(0, 41)
    acc[pk].push(data.value)
    return acc
  }, items)(stream(CONTXT_OI_ASSOC_STAT))

  const [included, excluded] = R.partition(([pk, stats]) => {
    stats.sort((a, b) => -a[1].localeCompare(b[1]))
    return (stats[0][0] !== 'END')
  })(Object.entries(items))

  const del = excluded.map(([pk]) => ({ type: 'del', key: `${CONTXT_OI_ASSOC}:${pk}`}))
  const put = included.map(([pk]) => ({ type: 'put', key: `${CONTXT_OI_ASSOC}:${pk}`, value: {}}))
  return Promise.all([db.batch(del), db.batch(put)])
}

const act_contxt = streams => async () => {
  const { db, stream, reduce, filter, map } = streams
  const include = data => data.value[0] === 'ISINCL'
  const pk = data => data.key.split(':')[1]
  const action = (acc, pk) => K(acc)(acc => acc[pk] = [])
  const actions = await R.compose(reduce(action, {}), map(pk), filter(include))(stream(ACT_CONTXT))

  await reduce((acc, data) => {
    const pk = data.key.split(':')[1].substring(0, 62)
    acc[pk].push(data.value)
    return acc
  }, actions)(stream(ACT_CONTXT_STAT))

  const [included, excluded] = R.partition(([pk, stats]) => {
    stats.sort((a, b) => -a[1].localeCompare(b[1]))
    return (stats[0][0] !== 'END')
  })(Object.entries(actions))

  const del = excluded.map(([pk]) => ({ type: 'del', key: `${ACT_CONTXT}:${pk}`}))
  const put = included.map(([pk]) => ({ type: 'put', key: `${ACT_CONTXT}:${pk}`, value: {}}))
  return Promise.all([db.batch(del), db.batch(put)])
}

module.exports = streams => ({
  contxt_oi_assoc: contxt_oi_assoc(streams),
  act_contxt: act_contxt(streams)
})