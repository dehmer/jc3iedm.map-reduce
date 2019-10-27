const R = require('ramda')
const { K } = require('./combinators')
const ent_id = require('../meta/ent_id.json')
const symbol_mapping = require('../meta/symbol-mapping.json')
const depth_cnt = require('../meta/depth_cnt.json')
const { STANDARD_IDENTITY, SIZE_CODE, STATUS } = require ('../meta/constants')

const coalesce = value => value || '[NULL]'
const replace = (position, s) => o => o.substring(0, position) + s + o.substring(position + s.length)

// NOTE: Order matters!
// E.g. ROUTE_TYPE must come before CTRL_FEAT_TYPE
const mappers = (() => {
  const xs = {}

  xs[ent_id.UNIT_TYPE] = value => {
    const type = value[ent_id.UNIT_TYPE]
    return [[0, 1, -2, -3, -4, -5].map(index => index < 0 ? coalesce(type[-index]) : type[index])]
  }

  xs[ent_id.CTRL_FEAT_TYPE] = value => [[value[ent_id.CTRL_FEAT_TYPE][0]]]
  xs[ent_id.FAC_TYPE] = value => [[value[ent_id.FAC_TYPE][0]]]
  xs[ent_id.HRBR_TYPE] = value => [[value[ent_id.HRBR_TYPE][0]]]

  xs[ent_id.ROUTE_TYPE] = value => {
    const type = value[ent_id.ROUTE_TYPE]
    const route = value[ent_id.ROUTE]
    if (route[0] && (type[0] === 'ALTSPL' || type[0] === 'MSR')) return [[type[0], route[0]]]
    else return [[type[0]]]
  }

  xs[ent_id.MIL_OBS_TYPE] = value => {
    const mil_obs_type = value[ent_id.MIL_OBS_TYPE]
    const fac_stat = value[ent_id.MIL_OBS_TYPE]
    const obj_item_hstly_stat = value[ent_id.OJ_ITEM_HSTLY_STAT]

    return [
      [mil_obs_type[0]],
      [mil_obs_type[0], mil_obs_type[1]],
      [mil_obs_type[0], '', fac_stat[1]]
      // TODO: remaining entries
    ]
  }

  xs[ent_id.TASK_FRMTN_TYPE] = value => {
    // Convoy is either moving or not.
    // TODO: add 'MOVING'/'HALTED' to descr_txt
    // "10000206:MLCNVY |" : "G*S*LCH---****X",
    // "10000206:MLCNVY |" : "G*S*LCM---****X"
    return []
  }

  return Object.entries(xs)
    .map(([ent_id, fn]) => [depth_cnt[ent_id], ent_id, fn])
    .sort((a, b) => b[0] - a[0])
    .map(([depth_cnt, ent_id, fn]) => [ent_id, fn])
})()

module.exports = async streams => {
  const { stream, map, reduce, filter, distinctOn, list, object, put } = streams

  const function_id = map(data => K(data)(data => {
    const mapper = mappers.find(([ent_id]) => data.value[ent_id])
    if (!mapper) return
    const [ent_id, fn] = mapper
    data.value.properties.sidc = fn(data.value)
      .map(xs => symbol_mapping[`${ent_id}:${xs.join(' | ')}`])
      .find(sidc => sidc)
  }))

  const sidc = map(data => K(data)(data => {
    const parts = []
    const obj_item_hstly_stat = data.value[ent_id.OBJ_ITEM_HSTLY_STAT]
    const unit_type = data.value[ent_id.UNIT_TYPE]
    const ctrl_feat_type = data.value[ent_id.CTRL_FEAT_TYPE]
    const rptd = data.value[ent_id.RPTD]

    // TODO: symbol modifier: HQ, TF, F/D

    if (obj_item_hstly_stat) parts[1] = STANDARD_IDENTITY[obj_item_hstly_stat[0]] || 'U'
    if (unit_type) parts[11] = SIZE_CODE[unit_type[6]] || '-'
    else if (ctrl_feat_type) parts[11] = SIZE_CODE[ctrl_feat_type[1]] || '-'
    if (rptd) parts[3] = STATUS[rptd[1]]

    data.value.properties.sidc =
      parts.reduce((sidc, part, index) => replace(index, part)(sidc), data.value.properties.sidc)
  }))

  const properties = map(data => K(data)(data => {
    const unit = data.value[ent_id.UNIT]
    const obj_item = data.value[ent_id.OBJ_ITEM]
    data.value.properties.t = unit && unit[0] || obj_item[1]
  }))

  const feature = map(data => {
    return {
      type: 'Feature',
      geometry: data.value.geometry,
      properties: data.value.properties
    }
  })

  const layer = reduce((layer, feature) => {
    layer.features.push(feature)
    return layer
  }, { type: 'FeatureCollection', features: []})

  const valid = filter(data => data.value.properties.sidc)
  const geometry = filter(data => data.value.geometry)
  const pipeline = R.compose(layer, feature, properties, sidc, valid, function_id, geometry, stream)
  const features = await pipeline(`P02:${ent_id.CONTXT_OI_ASSOC}`)
  console.log(JSON.stringify(features, null, 2))
}
