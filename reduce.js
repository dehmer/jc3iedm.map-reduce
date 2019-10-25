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

  /*
    'UNIT-TYPE' (10000081/10000129):
        UNIT_TYPE_cat_code
        UNIT_TYPE_arm_cat_code
        coalesce(UNIT_TYPE_arm_spclsn_code, '[NULL]')
        coalesce(UNIT_TYPE_suppl_spclsn_code, '[NULL]')
        coalesce(UNIT_TYPE_gen_mob_code, '[NULL]')
        coalesce(UNIT_TYPE_qual_code, '[NULL]')]
  */

  const coalesce = value => value || '[NULL]'
  const mapper = {}

  mapper[ent_id.UNIT_TYPE] = record => [0, 1, -2, -3, -4, -5].map(index => index < 0 ? coalesce(record[-index]) : record[index])
  mapper[ent_id.FAC_TYPE] = record => [record[0]]
  mapper[ent_id.CTRL_FEAT_TYPE] = record => [record[0]]
  mapper[ent_id.HRBR_TYPE] = record => [record[0]]

  const type = data => data.value[ent_id.OBJ_ITEM_TYPE]

  const function_id = data => K(data)(data => {
    Object.entries(mapper)
      .filter(([key]) => data.value[ent_id.OBJ_ITEM_TYPE][key])
      .map(([key, fn]) => {
        const descr_txt = fn(data.value[ent_id.OBJ_ITEM_TYPE][key]).join(' | ')
        const sidc = symbol_mapping[`${key}:${descr_txt}`]
        if (sidc) data.sidc = sidc
      })
  })

  const standard_id = data => K(data)(data => {
    if (!data.sidc) return
    if (!data.value[ent_id.OBJ_ITEM_HSTLY_STAT]) return
    data.sidc = replace(1, ID[data.value[ent_id.OBJ_ITEM_HSTLY_STAT]] || 'U')(data.sidc)
  })

  const echelon = data => K(data)(data => {
    if (!data.sidc) return
    const records = data.value[ent_id.OBJ_ITEM_TYPE]
    if (records[ent_id.UNIT_TYPE]) data.sidc = replace(10, ECHECLON[records[ent_id.UNIT_TYPE][6]] || '-')(data.sidc)
    else if (records[ent_id.CTRL_FEAT_TYPE]) data.sidc = replace(10, ECHECLON[records[ent_id.CTRL_FEAT_TYPE][1]] || '-')(data.sidc)
  })

  const status = data => K(data)(data => {
    if (!data.sidc) return
    if (!data.value[ent_id.RPTD]) return
    const codes = {
      ASS: 'A',
      PRDCTD: 'A',
      PLAN: 'A',
      REP: 'P',
      INFER: 'P'
    }

    const code = data.value[ent_id.RPTD][1]
    data.sidc = replace(3, codes[code] || '-')(data.sidc)
  })

  const xs = await R.compose(
    list,
    map(data => data.sidc),
    map(status),
    map(echelon),
    map(standard_id),
    map(function_id),
    filter(type),
    stream
  )(`TARGET:${ent_id.CONTXT_OI_ASSOC}`)
  console.log(xs)
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
