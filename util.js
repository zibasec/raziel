const Q_RE = /(?:"([^"]*)")/g
const RESERVED_WORD_RE = /#(\w+)/g

const NOTEXISTS = 'attribute_not_exists(hkey) AND attribute_not_exists(rkey)'
const HASPREFIX = 'hkey = :key and begins_with(rkey, :prefix)'

const ERR_KEY_LEN = new Error('Malformed key, expected [hash, range, ...]')
const ERR_KEY_TYPE = new Error('Expected an array')
const ERR_KEY_EMPTY = new Error('Hash or Range can not be empty')

const sleep = t => new Promise(resolve => setTimeout(resolve, t))
const clone = o => JSON.parse(JSON.stringify(o))

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
  clone,
  ERR_KEY_EMPTY,
  ERR_KEY_LEN,
  ERR_KEY_TYPE,
  HASPREFIX,
  NOTEXISTS,
  RESERVED_WORD_RE,
  Q_RE
}
