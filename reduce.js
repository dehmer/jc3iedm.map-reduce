#!/usr/bin/env node
const level = require('level')
const db = level('db', { valueEncoding: 'json' })
const streams = require('./lib/streams')(db)
const subtypes = require('./lib/subtypes')
const contxt_elmt = require('./lib/contxt_elmt')
const contxt_oi_assoc = require('./lib/contxt_oi_assoc')

const ready = []
const tasks = {
  'subtype': { reduce: subtypes, depends: [] },
  'contxt_elmt': { reduce: contxt_elmt, depends: [] },
  'contxt_oi_assoc': { reduce: contxt_oi_assoc, depends: []}
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
