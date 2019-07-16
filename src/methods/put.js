const { assertKey } = require('../util')
const dateAt = require('date-at')
const stringify = require('json-stringify-safe')

const NOTEXISTS = 'attribute_not_exists(hkey) AND attribute_not_exists(rkey)'
const api = module.exports = {}

api.put = async function put (key, opts, value, ...rest) {
  if (typeof value === 'undefined') {
    value = opts
    opts = null
  }

  const invalidKey = assertKey(key)
  if (invalidKey) return invalidKey

  const k = key.slice()
  let v = null

  try {
    v = JSON.parse(stringify(value))
  } catch (err) {
    return { err }
  }

  const params = {
    Item: {
      hkey: { S: k.shift() },
      rkey: { S: k.join(this.opts.sep) },
      value: { S: v }
    },
    TableName: (opts && opts.table) || this.table
  }

  for (const attribute of rest) {
    params.Item = { ...params.Item, ...attribute }
  }

  if (opts && opts.ttl) {
    let N = null

    if (typeof opts.ttl === 'number') {
      N = String(opts.ttl)
    } else {
      N = String(dateAt(opts.ttl).getTime() / 1000)
    }

    params.Item.ttl = {
      N
    }
  }

  if (opts && opts.notExists) {
    params.ConditionExpression = NOTEXISTS
  }

  try {
    await this.db.putItem(params).promise()
  } catch (err) {
    return { err }
  }

  return {}
}
