const AWS = require('aws-sdk')

const NOTEXISTS = 'attribute_not_exists(hkey) AND attribute_not_exists(rkey)'
const HASPREFIX = 'hkey = :key and begins_with(rkey, :prefix)'

let createdTable = process.env['NODE_ENV'] === 'production'

const ERR_KEY_LEN = new Error('Malformed key, expected [hash, range, ...]')
const ERR_KEY_TYPE = new Error('Expected an array')
const ERR_KEY_EMPTY = new Error('Hash or Range can not be empty')

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

class Table {
  constructor (table, opts) {
    this.table = table
    this.opts = opts || {}
    this.db = opts.db
  }

  then (resolve) {
    this.db = new AWS.DynamoDB(this.opts)

    const params = {
      AttributeDefinitions: [
        {
          AttributeName: 'hkey',
          AttributeType: 'S'
        },
        {
          AttributeName: 'rkey',
          AttributeType: 'S'
        }
      ],
      KeySchema: [
        {
          AttributeName: 'hkey',
          KeyType: 'HASH'
        },
        {
          AttributeName: 'rkey',
          KeyType: 'RANGE'
        }
      ],
      ProvisionedThroughput: {
        ReadCapacityUnits: 5,
        WriteCapacityUnits: 5
      },
      TableName: this.table
    }

    if (this.opts.encrypted) {
      params.SSESpecification = {
        Enabled: true
      }
    }

    if (this.opts.streaming) {
      params.StreamSpecification = {
        StreamEnabled: true,
        StreamViewType: this.opts.streaming
      }
    }

    const table = this

    if (!this.opts.createIfNotExists || createdTable) {
      return resolve({ db: this.db, table })
    }

    this.db.createTable(params, (err, data) => {
      if (err && err.name !== 'ResourceInUseException') {
        return resolve({ err })
      }

      createdTable = true

      resolve({ db: this.db, table })
    })
  }

  async put (key, opts = {}, value) {
    if (!value) {
      value = opts
      opts = null
    }

    const invalidKey = assertKey(key)
    if (invalidKey) return invalidKey

    const k = key.slice()
    let v = null

    try {
      v = JSON.stringify(value)
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

  async _gets (keys, opts = {}) {
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

      try {
        pair.value = JSON.parse(item.value.S)
      } catch (err) {
        pair.value = item.value.S
      }

      return pair
    })

    return { data: pairs }
  }

  async get (key, opts = {}) {
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

    if (!data || !data.Item) return { notFound: true }

    let value = null

    try {
      value = JSON.parse(data.Item.value.S)
    } catch (err) {
      return { err }
    }

    return { value }
  }

  async del (key, opts = {}) {
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

  query (opts = {}) {
    const params = {
      TableName: (opts && opts.table) || this.table,
      ProjectionExpression: 'hkey, rkey, #val',
      ExpressionAttributeNames: {
        '#val': 'value'
      }
    }

    opts.key = opts.key || []

    const key = opts.key.shift()
    const prefix = opts.key.join('/')

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

    const method = key ? 'query' : 'scan'
    const array = []
    const db = this.db

    let complete = false
    let i = 0

    const asyncIterator = Symbol.asyncIterator && !opts.legacy

    return {
      [Symbol.asyncIterator] () {
        return this
      },
      next: async () => {
        if (i < array.length) {
          const data = array[i++]

          if (asyncIterator) {
            return { value: data }
          }

          return {
            key: data.key,
            value: data.value,
            done: false
          }
        }

        if (complete) {
          return { done: true }
        }

        let res = null

        try {
          res = await db[method](params).promise()
        } catch (err) {
          if (asyncIterator) {
            throw err
          }
          return { err }
        }

        if (!res || (res.Items && !res.Items.length)) {
          return { done: true }
        }

        res.Items.map(item => {
          let value = null

          try {
            value = JSON.parse(item.value.S)
          } catch (err) {
            if (asyncIterator) {
              throw err
            }
            return { err }
          }

          const key = [item.hkey.S, item.rkey.S]
          array.push({ key, value })
        })

        if (typeof res.LastEvaluatedKey === 'undefined') {
          complete = true
        }

        const data = array[i++]

        if (asyncIterator) {
          return { value: data }
        }

        return {
          key: data.key,
          value: data.value,
          done: false
        }
      }
    }
  }

  async batch (ops, opts = {}) {
    const parseOp = op => {
      if (op.type === 'put') {
        let v = null

        try {
          v = JSON.stringify(op.value)
        } catch (err) {
          return { err }
        }

        return {
          PutRequest: {
            Item: {
              hkey: { S: op.key.shift() },
              rkey: { S: op.key.join(this.opts.sep) },
              value: { S: v }
            }
          }
        }
      }

      return {
        DeleteRequest: {
          Key: {
            hkey: { S: op.key.shift() },
            rkey: { S: op.key.join(this.opts.sep) }
          }
        }
      }
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
}

class Database {
  constructor (opts) {
    this.opts = opts || {}
    this.opts.region = this.opts.region || 'us-east-1'
    this.opts.sep = this.opts.sep || '/'

    if (this.opts.credentials) {
      AWS.config.credentials = opts.credentials
    }

    this.db = new AWS.DynamoDB(this.opts)
  }

  open (table, opts = {}) {
    if (!table) {
      throw new Error('table name required')
    }

    const _opts = Object.assign(this.opts, opts)

    return {
      async then (resolve) {
        resolve(await new Table(table, _opts))
      }
    }
  }
}

module.exports = Database
