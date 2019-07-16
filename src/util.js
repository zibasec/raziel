const ERR_KEY_LEN = new Error('Malformed key, expected [hash, range, ...]')
const ERR_KEY_TYPE = new Error('Expected an array')
const ERR_KEY_EMPTY = new Error('Hash or Range can not be empty')
const stringify = require('json-stringify-safe')

const sleep = t => new Promise(resolve => setTimeout(resolve, t))
const clone = o => JSON.parse(stringify(o))

const assertKey = key => {
  if (!Array.isArray(key)) {
    return { err: ERR_KEY_TYPE, key }
  }

  if (!(key.length >= 2)) {
    return { err: ERR_KEY_LEN, key }
  }

  if (!key[0] || !key[1]) {
    return { err: ERR_KEY_EMPTY, key }
  }
}

module.exports = {
  assertKey,
  sleep,
  clone
}
