const R = require('ramda')

module.exports = async streams => {
  const { db, stream, reduce } = streams

  const line_point = (acc, data) => {
    const pk = data.key.split(':')[2]
    const line_id = pk.split('|')[0]
    acc[line_id] = acc[line_id] || []
    acc[line_id][data.value[0] - 1] = data.value[1]
    return acc
  }

  const line_points = await R.compose(reduce(line_point, {}), stream)('X01:10000062')

  const geometry = (acc, data) => {
    const pk = data.key.split(':')[2]

    const mappers = {
      10000282: row => ({
        type: 'Point',
        coordinates: [row[1], row[0]]
      }),
      10000061: row => ({
        type: 'LineString',
        coordinates: line_points[pk].map(point => acc[point].coordinates)
      }),
      10000218: row => ({
        type: 'Polygon',
        coordinates: [line_points[row[0]].map(point => acc[point].coordinates)]
      }),
      10000214: row => ({
        type: 'LineString',
        coordinates: line_points[row[1]].map(point => acc[point].coordinates),
        geometry_width: row[0]
      }),
      10000048: row => ({
        type: 'Point',
        coordinates: acc[row[4]].coordinates,
        geometry_mnm_range: row[0],
        geometry_max_range: row[1],
        geometry_orient_angle: row[2],
        geometry_size_angle: row[3]
      }),
      10000220: row => acc[row[0]]
    }

    Object.entries(mappers)
      .filter(([ent_id]) => (data.value[ent_id]))
      .map(([ent_id, mapper]) => mapper(data.value[ent_id]))
      .forEach(x => (acc[pk] = x))

    return acc
  }

  const geometries = await R.compose(reduce(geometry, {}), stream)(`P01:10000063`)
  const ops = Object.entries(geometries).map(([pk, value]) => {
    // separate geometry and geometry properties:
    const geometry = { type: value.type, coordinates: value.coordinates}
    delete value.type
    delete value.coordinates
    const properties = { ...value }
    value = { geometry, properties }
    return { type: 'put', key: `P02:10000063:${pk}`, value }
  })
  return db.batch(ops)
}
