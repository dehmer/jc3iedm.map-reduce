#!/usr/bin/env node
const level = require('level')
const db = level('db', { valueEncoding: 'json' })
const streams = require('./lib/streams')(db)
const subtypes = require('./subtypes')(streams)
const geometries = require('./geometries')(streams)
const reports = require('./reports')(streams)
const { contxt_oi_assoc, act_contxt } = require('./context')(streams)

const ready = []
const tasks = {
  subtypes: {
    reduce: subtypes.reduce,
    depends: []
  },
  geometries: {
    reduce: geometries.reduce,
    depends: ['subtypes']
  },
  reports: {
    reduce: reports.reduce,
    depends: ['subtypes']
  },
  contxt_oi_assoc: {
    reduce: contxt_oi_assoc,
    depends: []
  },
  act_contxt: {
    reduce: act_contxt,
    depends: []
  }
}

const intersect = (a1, a2) => a1.filter(x => a2.includes(x))
const contains = (a1, a2) => intersect(a1, a2).length === a2.length

;(async () => {
  while(true) {
    const ps = Object.entries(tasks)
      .filter(([task, _]) => !ready.includes(task))
      .filter(([_, body]) => contains(ready, body.depends))
      .map(([task, body]) => body.reduce().then(() => task))

    const resolved = await Promise.all(ps)
    if (resolved.length === 0) break
    resolved.forEach(task => ready.push(task))
  }
})()
