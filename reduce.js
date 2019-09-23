#!/usr/bin/env node
const level = require('level')
const db = level('db', { valueEncoding: 'json' })
const subtypes = require('./ent_subt.json')

const CORRIDOR_AREA = '10000214'
const FAN_AREA = 10000048
const GEO_POINT = '10000282'
const LINE = '10000061'
const LINE_POINT = '10000062'
const LOC = '10000063'
const POLYGON_AREA = '10000218'

const entityStream = entityId => db.createReadStream({
  gt: entityId,
  lt: `${entityId}\xff`
})

const reduce = (stream, fn, initial) => new Promise(resolve => {
  let acc = initial
  stream
    .on('data', data => (acc = fn(acc, data)))
    .on('end', () => resolve(acc))
})

const reduceSubtypeHierarchy = async entityId => {
  const xs = await reduce(entityStream(entityId), (acc, data) => {
    const [entityId, pk] = data.key.split(':')
    acc[pk] = acc[pk] || {}
    acc[pk][entityId] = data.value.split('|')
    return acc
  }, {})

  const ps = subtypes[entityId].map(entityId => {
    return reduce(entityStream(entityId), (acc, data) => {
      const [entityId, pk] = data.key.split(':')
      acc[pk][entityId] = data.value.split('|')
      return acc
    }, xs)
  })

  await Promise.all(ps)
  const ops = Object.entries(xs).map(([pk, rows]) => ({ type: 'put', key: `${entityId}:${pk}`, value: rows }))
  return db.batch(ops)
}

const reduceAllSubtypeHierarchy = () =>
  Object.keys(subtypes).map(entityId => reduceSubtypeHierarchy(entityId))


const mapLocations = async () => {

  // Load line points:
  const linePoints = await reduce(entityStream(LINE_POINT), (acc, data) => {
    const pk = data.key.split(':')[1]
    const lineId = pk.split('|')[0]
    const fields = data.value.split('|')
    acc[lineId] = acc[lineId] || []
    acc[lineId][fields[0] - 1] = fields[1]
    return acc
  }, {})

  const geometries = await reduce(entityStream(LOC), (acc, data) => {
    const [entityId, pk] = data.key.split(':')

    const point = row => ({ type: 'Point', coordinates: [row[1], row[0]]})

    const line = () => ({
      type: 'LineString',
      coordinates: linePoints[pk].map(linePoint => acc[linePoint].coordinates)
    })

    const polygonArea = row => ({
      type: 'Polygon',
      coordinates: [linePoints[row[0]].map(linePoint => acc[linePoint].coordinates)]
    })

    const corridorArea = row => ({
      type: 'LineString',
      coordinates: linePoints[row[1]].map(linePoint => acc[linePoint].coordinates),
      width: row[0]
    })

    const fanArea = row => ({
      type: 'Point',
      coordinates: acc[row[4]].coordinates,
      minRange: row[0],
      maxRange: row[1],
      startAngle: row[2],
      sizeAngle: row[3]
    })

    const geometry = value => {
      if (value[GEO_POINT]) return point(value[GEO_POINT])
      else if (value[LINE]) return line()
      else if (value[POLYGON_AREA]) return polygonArea(value[POLYGON_AREA])
      else if (value[CORRIDOR_AREA]) return corridorArea(value[CORRIDOR_AREA])
      else if (value[FAN_AREA]) return fanArea(value[FAN_AREA])
      console.log('unsupported geometry', value)
    }

    const x = geometry(data.value)
    if (x) acc[pk] = x
    else acc[pk] = data.value
    return acc
  }, {})

  const ops = Object.entries(geometries).map(([pk, value]) => ({ type: 'put', key: `${LOC}:${pk}`, value }))
  return db.batch(ops)
}

;(async () => {
  await Promise.all(reduceAllSubtypeHierarchy())
  await mapLocations()
})()

