const R = require('ramda')
const ent_id = require('../meta/ent_id.json')

module.exports = async streams => {

  // contxt_oi_assoc: contxt_id:obj_item_id -> {}
  const { db, stream, filter, map, reduce, list, object } = streams

  const coia_kv = data => [data.key.split(':')[2], {}]
  const mat_coia_recs = R.compose(object, map(coia_kv), stream)
  const coia_recs = await mat_coia_recs(`TARGET:${ent_id.CONTXT_OI_ASSOC}`)

  // contxt_elmt: rptd_id -> contxt_id
  const ce_kv = data => [data.value[0], data.key.split(':')[2].split('|')[0]]
  const mat_ce_recs = R.compose(object, map(ce_kv), stream)
  const ce_recs = await mat_ce_recs(`TARGET:${ent_id.CONTXT_ELMT}`)

  // OBJ_ITEM_HSTLY_STAT =>
  const oihsts_in_contxt = data => {
    data.contxt_id = ce_recs[data.value[1]]
    if (!data.contxt_id) return false
    data.obj_item_id = data.key.split(':')[1].split('|')[0]
    return coia_recs[`${data.contxt_id}|${data.obj_item_id}`]
  }

  const merge_oihsts_rec = (coia_recs, data) => {
    coia_recs[`${data.contxt_id}|${data.obj_item_id}`].OIHSTS = data.value[0]
    return coia_recs
  }

  await R.compose(reduce(merge_oihsts_rec, coia_recs), filter(oihsts_in_contxt), stream)(`${ent_id.OBJ_ITEM_HSTLY_STAT}`)

  // OBJ_ITEM_LOC/LOC =>
  const loc_kv = data => [data.key.split(':')[2], data.value]
  const loc_recs = await R.compose(object, map(loc_kv), stream)(`TARGET:${ent_id.LOC}`)

  const oiloca_in_contxt = data => {
    data.contxt_id = ce_recs[data.value[13]]
    if (!data.contxt_id) return false
    data.obj_item_id = data.key.split(':')[1].split('|')[0]
    return coia_recs[`${data.contxt_id}|${data.obj_item_id}`]
  }

  const merge_oiloca_rec = (coia_recs, data) => {
    const loc_id = data.key.split(':')[1].split('|')[1]
    coia_recs[`${data.contxt_id}|${data.obj_item_id}`].OILOCA = loc_recs[loc_id]
    if (data.value[2]) coia_recs[`${data.contxt_id}|${data.obj_item_id}`].brng_angle = data.value[2]
    if (data.value[8]) coia_recs[`${data.contxt_id}|${data.obj_item_id}`].speed_rate = data.value[8]
    return coia_recs
  }

  const oiloca_recs = await R.compose(reduce(merge_oiloca_rec, coia_recs), filter(oiloca_in_contxt), stream)(`${ent_id.OBJ_ITEM_LOC}`)

  const op = ([key, value]) => ({ type: 'put', key: `TARGET:${ent_id.CONTXT_OI_ASSOC}:${key}`, value})
  const ops = Object.entries(oiloca_recs).map(op)
  return db.batch(ops)
}
