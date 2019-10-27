const R = require('ramda')
const { K } = require('./combinators')
const ent_id = require('../meta/ent_id.json')

module.exports = async streams => {
  const { db, stream, filter, map, reduce, object } = streams

  const entities = {}
  entities[`P01:${ent_id.CONTXT_OI_ASSOC}`] = data => [data.key.split(':')[2], {}]
  entities[`P01:${ent_id.CONTXT_ELMT}`] = data => [data.value[0], data.key.split(':')[2].split('|')[0]]
  entities[`P02:${ent_id.LOC}`] = data => [data.key.split(':')[2], data.value]
  entities[`P01:${ent_id.OBJ_TYPE}`] = data => [data.key.split(':')[2], data.value]
  entities[`P01:${ent_id.RPTD}`] = data => [data.key.split(':')[2], data.value]

  const mat_object = kv => R.compose(object, map(kv), stream)
  const mat_entities = Object.entries(entities).map(([partition, kv]) => mat_object(kv)(partition))
  const [coia, ce, loc, type, rptd] = await Promise.all(mat_entities)

  // obj_item_id -> [contxt_id]
  const items = Object.keys(coia)
    .map(key => key.split('|'))
    .reduce((acc, [contxt_id, obj_item_id]) => {
      acc[obj_item_id] = acc[obj_item_id] || []
      acc[obj_item_id].push(contxt_id)
      return acc
    }, {})

  const include = data => items[data.key.split(':')[2]]
  const merge = (coia, data) => K(coia)(coia => {
    const obj_item_id = data.key.split(':')[2]
    ;(items[obj_item_id] || []).forEach(contxt_id => {
      coia[`${contxt_id}|${obj_item_id}`] = data.value
    })
  })

  await R.compose(reduce(merge, coia), filter(include), stream)(`P01:${ent_id.OBJ_ITEM}`)

  const elements = {}

  elements[`X01:${ent_id.OBJ_ITEM_HSTLY_STAT}`] = {
    rptd_id: data => data.value[1],
    obj_item_id: data => data.key.split(':')[2].split('|')[0],
    merge: (coia, data) => K(coia)(coia => {
      coia[data.pk][ent_id.OBJ_ITEM_HSTLY_STAT] = data.value
    })
  }

  elements[`X01:${ent_id.OBJ_ITEM_LOC}`] = {
    rptd_id: data => data.value[13],
    obj_item_id: data => data.key.split(':')[2].split('|')[0],
    merge: (coia, data) => K(coia)(coia => {
      const loc_id = data.key.split(':')[2].split('|')[1]
      coia[data.pk] = {...coia[data.pk], ...loc[loc_id]}
      coia[data.pk] = {...coia[data.pk], ...rptd[data.rptd_id]}
      if (data.value[2]) coia[data.pk].properties.q = data.value[2]
      if (data.value[8]) coia[data.pk].properties.z = data.value[8]
    })
  }

  elements[`P01:${ent_id.OBJ_ITEM_STAT}`] = {
    rptd_id: data => data.value[ent_id.OBJ_ITEM_STAT][3],
    obj_item_id: data => data.key.split(':')[2].split('|')[0],
    merge: (coia, data) => K(coia)(coia => {
      coia[data.pk] = {...coia[data.pk], ...data.value}
    })
  }

  elements[`X01:${ent_id.OBJ_ITEM_TYPE}`] = {
    rptd_id: data => data.value[0],
    obj_item_id: data => data.key.split(':')[2].split('|')[0],
    merge: (coia, data) => K(coia)(coia => {
      const type_id = data.key.split(':')[2].split('|')[1]
      coia[data.pk] = {...coia[data.pk], ...type[type_id]}
    })
  }

  const ps = Object.entries(elements).map(([partition, fns]) => {
    const include = filter(data => {
      data.rptd_id = fns.rptd_id(data)
      const contxt_id = ce[data.rptd_id]
      if (!contxt_id) return false // element not in context
      const obj_item_id = fns.obj_item_id(data)
      data.pk = `${contxt_id}|${obj_item_id}`
      return coia[data.pk] // object item in context?
    })

    return R.compose(reduce(fns.merge, coia), include, stream)(partition)
  })

  await Promise.all(ps)
  const op = ([key, value]) => ({ type: 'put', key: `P02:${ent_id.CONTXT_OI_ASSOC}:${key}`, value})
  const ops = Object.entries(coia).map(op)
  return db.batch(ops)
}
