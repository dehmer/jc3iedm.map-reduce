#!/usr/bin/env node
const level = require('level')
const db = level('db', { valueEncoding: 'json' })
const streams = require('./lib/streams')(db)
const subtypes = require('./lib/subtypes')
const contxt_elmt = require('./lib/contxt_elmt')
const contxt_oi_assoc = require('./lib/contxt_oi_assoc')
const loc = require('./lib/loc')
const obj_item = require('./lib/obj_item')

const R = require('ramda')
const { K } = require('./lib/combinators')
const ent_id = require('./meta/ent_id.json')
const symbol_mapping = require('./meta/symbol-mapping.json')

const ECHECLON = {
  'TEAM': 'A',
  'SQUAD': 'B',
  'SECT': 'C',
  'PLT': 'D',
  'COY': 'E',
  'BN': 'F',
  'SQDRNA': 'F',
  'SQDRNM': 'F',
  'RGT': 'G',
  'BDE': 'H',
  'BDEGRP': 'H',
  'DIV': 'I',
  'CORPS': 'J',
  'ARMY': 'K',
  'AG': 'L',
  'REGION': 'M'
}

const ID = {
  'FR': 'F',
  'AFR': 'A',
  'NEUTRL': 'N',
  'FAKER': 'K',
  'HO': 'H',
  'JOKER': 'J',
  'AHO': 'S',
  'SUSPCT': 'S',
  'PENDNG': 'P',
  'ANT': 'P',
  'IV': 'P',
  'AIV': 'P'
}

const replace = (position, s) => o => o.substring(0, position) + s + o.substring(position + s.length)

const sidc = async streams => {
  const { db, stream, filter, map, reduce, object, list } = streams

  const coalesce = value => value || '[NULL]'
  const mapper = {}

  mapper[ent_id.UNIT_TYPE] = value => {
    const type = value[ent_id.OBJ_ITEM_TYPE][ent_id.UNIT_TYPE]
    return [0, 1, -2, -3, -4, -5].map(index => index < 0 ? coalesce(type[-index]) : type[index])
  }

  mapper[ent_id.FAC_TYPE] = value => {
    const type = value[ent_id.OBJ_ITEM_TYPE][ent_id.FAC_TYPE]
    return [type[0]]
  }

  mapper[ent_id.CTRL_FEAT_TYPE] = value => {
    const type = value[ent_id.OBJ_ITEM_TYPE][ent_id.CTRL_FEAT_TYPE]
    return [type[0]]
  }

  mapper[ent_id.HRBR_TYPE] = value => {
    const type = value[ent_id.OBJ_ITEM_TYPE][ent_id.HRBR_TYPE]
    return [type[0]]
  }

  mapper[ent_id.ROUTE_TYPE] = value => {
    const type = value[ent_id.OBJ_ITEM_TYPE][ent_id.ROUTE_TYPE]
    const route = value[ent_id.OBJ_ITEM][ent_id.ROUTE]
    if (route[0] && (type[0] === 'ALTSPL' || type[0] === 'MSR')) return [type[0], route[0]]
    else return [type[0]]
  }

  mapper[ent_id.TASK_FRMTN_TYPE] = value => {
    // Convoy is either moving or not.
    // TODO: add 'MOVING'/'HALTED' to descr_txt
    // "10000206:MLCNVY |" : "G*S*LCH---****X",
    // "10000206:MLCNVY |" : "G*S*LCM---****X"
    return []
  }

  const type = data => data.value[ent_id.OBJ_ITEM_TYPE]

  const function_id = data => K(data)(data => {
    data.properties = {}
    Object.entries(mapper)
      .filter(([key]) => data.value[ent_id.OBJ_ITEM_TYPE][key])
      .map(([key, fn]) => {
        const descr_txt = fn(data.value).join(' | ')
        const sidc = symbol_mapping[`${key}:${descr_txt}`]
        if (sidc) data.properties.sidc = sidc
      })
  })

  const standard_id = data => K(data)(data => {
    if (!data.properties.sidc) return
    if (!data.value[ent_id.OBJ_ITEM_HSTLY_STAT]) return
    data.properties.sidc = replace(1, ID[data.value[ent_id.OBJ_ITEM_HSTLY_STAT]] || 'U')(data.properties.sidc)
  })

  const echelon = data => K(data)(data => {
    if (!data.properties.sidc) return
    const records = data.value[ent_id.OBJ_ITEM_TYPE]
    if (records[ent_id.UNIT_TYPE]) data.properties.sidc = replace(10, ECHECLON[records[ent_id.UNIT_TYPE][6]] || '-')(data.properties.sidc)
    else if (records[ent_id.CTRL_FEAT_TYPE]) data.properties.sidc = replace(10, ECHECLON[records[ent_id.CTRL_FEAT_TYPE][1]] || '-')(data.properties.sidc)
  })

  const status = data => K(data)(data => {
    if (!data.properties.sidc) return
    if (!data.value[ent_id.RPTD]) return
    const codes = {
      ASS: 'A',
      PRDCTD: 'A',
      PLAN: 'A',
      REP: 'P',
      INFER: 'P'
    }

    const code = data.value[ent_id.RPTD][1]
    data.properties.sidc = replace(3, codes[code] || '-')(data.properties.sidc)
  })

  const geometry = data => K(data)(data => {
    if (!data.value[ent_id.OBJ_ITEM_LOC]) return

    const record = data.value[ent_id.OBJ_ITEM_LOC]
    data.geometry = {}
    data.geometry.type = data.value[ent_id.OBJ_ITEM_LOC].type
    data.geometry.coordinates = data.value[ent_id.OBJ_ITEM_LOC].coordinates

    if (record.geometry_mnm_range)    data.properties.geometry_mnm_range    = record.geometry_mnm_range
    if (record.geometry_max_range)    data.properties.geometry_max_range    = record.geometry_max_range
    if (record.geometry_orient_angle) data.properties.geometry_orient_angle = record.geometry_orient_angle
    if (record.geometry_size_angle)   data.properties.geometry_size_angle   = record.geometry_size_angle
    if (record.geometry_width)        data.properties.geometry_width        = record.geometry_width
    if (record.brng_angle)            data.properties.q                     = record.brng_angle
    if (record.speed_rate)            data.properties.z                     = record.speed_rate
  })

  const properties = data => K(data)(data => {
    data.properties.t = data.value[ent_id.OBJ_ITEM][ent_id.OBJ_ITEM][1]
  })

  const feature = data => ({
    type: 'Feature',
    geometry: data.geometry,
    properties: data.properties
  })

  const validFeature = feature => {
    if (!feature.geometry) return false
    return true
  }

  const features = await R.compose(
    list,
    filter(validFeature),
    map(feature),
    map(properties),
    map(geometry),
    map(status),
    map(echelon),
    map(standard_id),
    map(function_id),
    filter(type),
    stream
  )(`TARGET:${ent_id.CONTXT_OI_ASSOC}`)

  console.log(JSON.stringify({
    type: 'FeatureCollection',
    features: features
  }, null, 2))
}

const ready = []
const tasks = {
  'subtype': { reduce: subtypes, depends: [] },
  'contxt_elmt': { reduce: contxt_elmt, depends: [] },
  'contxt_oi_assoc': { reduce: contxt_oi_assoc, depends: []},
  'loc': { reduce: loc, depends: ['subtype'] },
  'obj_item': { reduce: obj_item, depends: ['loc', 'contxt_elmt', 'contxt_oi_assoc'] },
  'sidc': { reduce: sidc, depends: ['obj_item']}
}

const intersect = (a1, a2) => a1.filter(x => a2.includes(x))
const contains = (a1, a2) => intersect(a1, a2).length === a2.length

;(async () => {
  while(true) {
    const ps = Object.entries(tasks)
      .filter(([task, _]) => !ready.includes(task))
      .filter(([_, body]) => contains(ready, body.depends))
      .map(([task, body]) => body.reduce(streams).then(() => task))

    const resolved = await Promise.all(ps)
    if (resolved.length === 0) break
    resolved.forEach(task => ready.push(task))
  }
})()
