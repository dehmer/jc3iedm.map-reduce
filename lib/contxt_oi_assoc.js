const R = require('ramda')

module.exports = async streams => {
  const { stream, map, filter, distinctOn, object, put } = streams

  const recent = distinctOn(
    data => data.key.split(':')[2].substring(0, 41),
    (a, b) => b.value[1].localeCompare(a.value[1])
  )

  const start = filter(data => data.value[0] === 'START')
  const kv = map(data => [data.key.split(':')[2].substring(0, 41), true])
  const coia = await R.compose(object, kv, start, recent, stream)('X01:10000299')
  const include = filter(data => coia[data.key.split(':')[2]])
  const target = map(data => ({ key: `P01:${data.key.substring(4)}`, value: data.value }))
  await R.compose(put, target, include, stream)('X01:10000137')
}
