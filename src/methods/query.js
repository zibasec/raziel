const Q_RE = /(?:"([^"]*)")/g
const RESERVED_WORD_RE = /#(\w+)/g
const HASPREFIX = 'hkey = :key and begins_with(rkey, :prefix)'

const api = module.exports = {}

api.query = function query (opts = {}, filter) {
  const params = {
    TableName: (opts && opts.table) || this.table
  }

  const collectValues = (_, S) => {
    const id = `:${this.uuid()}`
    params.ExpressionAttributeValues[id] = { S }
    return id
  }

  const collectNames = (_, S) => {
    const id = `#${S}`
    params.ExpressionAttributeNames[id] = { S }
    return id
  }

  if (typeof opts === 'string') {
    const q = opts
    opts = { method: 'query' }

    params.ExpressionAttributeValues = {}
    params.KeyConditionExpression = q
      .replace(RESERVED_WORD_RE, collectNames)
      .replace(Q_RE, collectValues)
  } else {
    params.ProjectionExpression = 'hkey, rkey, #val'
    params.ExpressionAttributeNames = { '#val': 'value' }
  }

  if (filter) {
    params.FilterExpression = filter
      .replace(RESERVED_WORD_RE, collectNames)
      .replace(Q_RE, collectValues)
  }

  if (opts.ttl) {
    params.ProjectionExpression = 'hkey, rkey, #myttl, #val'
    params.FilterExpression = '#myttl > :ttl'
    params.ExpressionAttributeValues = {
      ':ttl': { N: '0' }
    }
    params.ExpressionAttributeNames = {
      '#val': 'value',
      '#myttl': 'ttl'
    }
  }

  opts.key = opts.key || []

  let key = opts.key.shift()
  let prefix = opts.key.join('/')

  if (prefix) {
    params.ExpressionAttributeValues = {
      ':key': { S: key },
      ':prefix': { S: prefix }
    }
    params.KeyConditionExpression = HASPREFIX
  } else if (key) {
    params.ExpressionAttributeValues = {
      ':key': { S: key }
    }
    params.KeyConditionExpression = 'hkey = :key'
  }

  if (opts.limit) {
    params.Limit = opts.limit
  }

  if (opts.start) {
    params.ExclusiveStartKey = opts.start
  }

  const method = opts.method || key ? 'query' : 'scan'
  const array = []
  const db = this.db

  let complete = false
  let i = 0

  let start = null

  return {
    [Symbol.asyncIterator] () {
      return this
    },
    next: async () => {
      if (i < array.length) {
        const data = array[i++]
        return { value: data }
      }

      if (complete) {
        return { start, done: true }
      }

      let res = null

      try {
        res = await db[method](params).promise()
      } catch (err) {
        throw err
      }

      if (!res || (res.Items && !res.Items.length)) {
        return { start, done: true }
      }

      res.Items.map(item => {
        let value = null
        let ttl = null

        try {
          value = JSON.parse(item.value.S)

          if (opts.ttl) {
            ttl = Number(JSON.parse(item.ttl.N))
          }
        } catch (err) {
          throw err
        }

        const key = [item.hkey.S, item.rkey.S]
        array.push({ ...item, key, value, ttl })
      })

      if (typeof res.LastEvaluatedKey === 'undefined' || opts.limit) {
        start = res.LastEvaluatedKey
        complete = true
      } else {
        params.ExclusiveStartKey = res.LastEvaluatedKey
      }

      const data = array[i++]

      return { value: data }
    }
  }
}
