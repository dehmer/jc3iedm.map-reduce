const CORRIDOR_AREA = '10000214'
const FAN_AREA = 10000048
const GEO_POINT = '10000282'
const LINE = '10000061'
const LINE_POINT = '10000062'
const LOC = '10000063'
const POLYGON_AREA = '10000218'
const SURF_VOL = '10000220'

const reduce = streams => async () => {
  const { db, stream, reduce, map, list } = streams

  // Load line points:
  const linePoint = (acc, data) => {
    const pk = data.key.split(':')[1]
    const lineId = pk.split('|')[0]
    acc[lineId] = acc[lineId] || []
    acc[lineId][data.value[0] - 1] = data.value[1]
    return acc
  }

  // line_point_id => Ord([point_id])
  const linePoints = await reduce(linePoint, {})(stream(LINE_POINT))

  const reduceLocation = (acc, data) => {
    const [_, pk] = data.key.split(':')

    const point = row => ({ type: 'Point', coordinates: [row[1], row[0]]})

    const line = row => ({
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

    // Use the defining volume.
    const surfaceVolume = row => acc[row[0]]

    const geometry = value => {
      if (value[GEO_POINT]) return point(value[GEO_POINT])
      else if (value[LINE]) return line(value[LINE])
      else if (value[POLYGON_AREA]) return polygonArea(value[POLYGON_AREA])
      else if (value[CORRIDOR_AREA]) return corridorArea(value[CORRIDOR_AREA])
      else if (value[FAN_AREA]) return fanArea(value[FAN_AREA])
      else if (value[SURF_VOL]) return surfaceVolume(value[SURF_VOL])
      console.log('unsupported geometry', value)
    }

    const x = geometry(data.value)
    if (x) acc[pk] = x
    else acc[pk] = data.value

    return acc
  }

  const geometries = await reduce(reduceLocation, {})(stream(LOC))
  const ops = Object.entries(geometries).map(([pk, value]) => ({ type: 'put', key: `${LOC}:${pk}`, value }))
  return db.batch(ops)
}

module.exports = streams => ({
  reduce: reduce(streams)
})