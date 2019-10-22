const K = v => fn => { fn(v); return v }
const kv = (k, v) => K({})(o => o[k] = v)
module.exports = { K, kv }
