const R = require('ramda')

module.exports = async streams => {
  const { stream, map, filter, distinctOn, object, put } = streams

  // DISTINCT ON contxt_id, contxt_elmt_ix
  // ORDER BY contxt_id, contxt_elmt_ix, effctv_dttm DESC
  const recent = distinctOn(
    data => data.key.split(':')[2].substring(0, 41),
    (a, b) => b.value[1].localeCompare(a.value[1])
  )

  const additn = filter(data => data.value[0] === 'ADDITN')
  const kv = map(data => [data.key.split(':')[2].substring(0, 41), true])
  const ces = await R.compose(object, kv, additn, recent, stream)('X01:10000298')
  const include = filter(data => ces[data.key.split(':')[2]])
  const target = map(data => ({ key: `P01:${data.key.substring(4)}`, value: data.value }))
  await R.compose(put, target, include, stream)('X01:10000168')
}
