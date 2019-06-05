const { assertKey } = require('../util')

const api = module.exports = {}

api.del = async function del (key, opts = {}) {
  const invalidKey = assertKey(key)
  if (invalidKey) return invalidKey

  const k = key.slice()
  const params = {
    Key: {
      hkey: { S: k.shift() },
      rkey: { S: k.join(this.opts.sep) }
    },
    TableName: (opts && opts.table) || this.table
  }

  try {
    await this.db.deleteItem(params).promise()
  } catch (err) {
    return { err }
  }

  return {}
}
