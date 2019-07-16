const dateAt = require('date-at')
const { clone } = require('../util')
const stringify = require('json-stringify-safe')

const api = module.exports = {}

api.batch = async function batch (ops, opts = {}) {
  const parseOp = op => {
    op = clone(op)
    const hkey = { S: op.key.shift() }
    const rkey = { S: op.key.join(this.opts.sep) }

    if (op.type === 'put') {
      const ttl = op.ttl
      let value = op.value

      delete op.value
      delete op.ttl
      delete op.type
      delete op.key

      try {
        value = { S: stringify(value) }
      } catch (err) {
        return { err }
      }

      const o = {
        PutRequest: {
          Item: {
            hkey,
            rkey,
            value,
            ...op // add the rest
          }
        }
      }

      if (ttl) {
        let N = null

        if (typeof ttl === 'number') {
          N = String(ttl)
        } else {
          N = String(dateAt(ttl).getTime() / 1000)
        }

        o.PutRequest.Item.ttl = { N }
      }

      return o
    }

    return { DeleteRequest: { Key: { hkey, rkey } } }
  }

  const table = (opts && opts.table) || this.table

  const params = {
    RequestItems: {
      [table]: ops.map(parseOp)
    }
  }

  try {
    await this.db.batchWriteItem(params).promise()
  } catch (err) {
    return { err }
  }

  return {}
}
