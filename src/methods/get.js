const { assertKey } = require('../util')
const api = module.exports = {}

api._gets = async function _gets (keys, opts = {}) {
  const tableName = (opts && opts.table) || this.table
  const spec = { Keys: [] }

  const params = {
    RequestItems: {
      [tableName]: spec
    }
  }

  spec.Keys = keys.slice().map(key => {
    return {
      hkey: { S: key.shift() },
      rkey: { S: key.join(this.opts.sep) }
    }
  })

  let data = null

  try {
    data = await this.db.batchGetItem(params).promise()
  } catch (err) {
    return { err }
  }

  let pairs = []

  if (!data.Responses[tableName].length) {
    return { data: pairs }
  }

  pairs = data.Responses[tableName].map(item => {
    const pair = { key: [item.hkey.S, item.rkey.S] }

    if (item.ttl) {
      const then = new Date(Number(item.ttl.N) * 1000)
      const now = new Date()
      if (then <= now) return null
    }

    try {
      pair.value = JSON.parse(item.value.S)
    } catch (err) {
      pair.value = item.value.S
    }

    return pair
  })
  return { data: pairs.filter(Boolean) }
}

api.get = async function get (key, opts = {}) {
  if (Array.isArray(key[0])) {
    return this._gets(key, opts)
  }

  const k = key.slice()

  const invalidKey = assertKey(key)
  if (invalidKey) return invalidKey

  const params = {
    TableName: (opts && opts.table) || this.table,
    Key: {
      hkey: { S: k.shift() },
      rkey: { S: k.join(this.opts.sep) }
    }
  }

  let data = null

  try {
    data = await this.db.getItem(params).promise()
  } catch (err) {
    return { err }
  }

  if (data && data.Item && data.Item.ttl) {
    const then = new Date(Number(data.Item.ttl.N) * 1000)
    const now = new Date()
    if (then <= now) data = null
  }

  if (!data || !data.Item) {
    return { err: { notFound: true } }
  }

  let value = null

  try {
    value = JSON.parse(data.Item.value.S)
  } catch (err) {
    return { err }
  }

  return { value }
}
