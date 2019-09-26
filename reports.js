const R = require('ramda')
const RPTD = 10000157
const RPTD_ABS_TIMING = 10000154

const reduce = streams => async () => {
  const { db, stream, map, list } = streams

  const rptd = data => ([data.key.split(':')[1], {
    cat_code: data.value[RPTD][1],
    rep_dttm: data.value[RPTD][5],
    effctv_start_dttm: data.value[RPTD_ABS_TIMING] ? data.value[RPTD_ABS_TIMING][0] : '',
    effctv_end_dttm: data.value[RPTD_ABS_TIMING] ? data.value[RPTD_ABS_TIMING][1] : ''
  }])

  const op = ([key, value]) => ({ type: 'put', key: `${RPTD}:${key}`, value })
  const ops = await R.compose(list, map(op), map(rptd))(stream(RPTD))
  return db.batch(ops)
}

module.exports = streams => ({
  reduce: reduce(streams)
})