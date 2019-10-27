#!/usr/bin/env node
const level = require('level')
const db = level('db', { valueEncoding: 'json' })
const streams = require('./lib/streams')(db)
const subtypes = require('./lib/substypes')
const contxt_elmt = require('./lib/contxt_elmt')
const contxt_oi_assoc = require('./lib/contxt_oi_assoc')
const geometry = require('./lib/geometry')
const obj_item = require('./lib/obj_item')
const feature = require('./lib/feature')

const ready = []
const tasks = {
  'subtypes': { reduce: subtypes, depends: []},
  'contxt_elmt': { reduce: contxt_elmt, depends: [] },
  'contxt_oi_assoc': { reduce: contxt_oi_assoc, depends: [] },
  'geometry': { reduce: geometry, depends: ['subtypes'] },
  'obj_item': { reduce: obj_item, depends: ['geometry'] },
  'feature': { reduce: feature, depends: [ /* 'obj_item' */ ] }
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
