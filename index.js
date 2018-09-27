const AWS = require('aws-sdk')
const { createServer } = require('net')

const NOTEXISTS = 'attribute_not_exists(hkey) AND attribute_not_exists(rkey)'
const HASPREFIX = 'hkey = :key and begins_with(rkey, :prefix)'

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
    this.waitFor = opts.waitFor || false
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

    if (!this.opts.createIfNotExists) {
      return resolve({ db: this.db, table })
    }

    this.db.createTable(params, async (err, data) => {
      if (err && err.name !== 'ResourceInUseException') {
        return resolve({ err })
      }

      if (this.waitFor) {
        try {
          await this.db.waitFor('tableExists', { TableName: this.table }).promise()
          resolve({ db: this.db, table })
        } catch (err) {
          resolve({ err })
        }
      } else {
        resolve({ db: this.db, table })
      }
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

  async count (opts, progress) {
    const params = {
      TableName: (opts && opts.table) || this.table,
      Select: 'COUNT'
    }

    let count = 0

    while (true) {
      let data = null

      try {
        data = await this.db.scan(params).promise()
      } catch (err) {
        return { err }
      }

      count += data.Count

      if (!data.LastEvaluatedKey) break
      params.ExclusiveStartKey = data.LastEvaluatedKey
      progress()
    }

    return { count }
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
        } else {
          params.ExclusiveStartKey = res.LastEvaluatedKey
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

  async localDynamoAvailable (port) {
    try {
      const portTaken = async (port) => new Promise((resolve, reject) => {
        const scanner = createServer()
          .once('error', err => {
            if (err.code === 'EADDRINUSE') {
              resolve(true)
            }
            reject(err)
          })
          .once('listening', () => {
            scanner.once('close', () => {
              resolve(false)
            }).close()
          })
          .listen(port)
      })

      const isAvailable = await portTaken(port)
      return { isAvailable: Boolean(isAvailable) }
    } catch (err) {
      return { err }
    }
  }

  async open (table, opts = {}) {
    if (!table) {
      throw new Error('table name required')
    }

    if (process.env['LOCAL_DYNAMO']) {
      const dynamoPort = process.env['LOCAL_DYNAMO_PORT'] || 8000
      const { isAvailable } = await this.localDynamoAvailable(dynamoPort)

      if (isAvailable) {
        opts.endpoint = `http://localhost:${dynamoPort}`
      } else {
        return {
          err: new Error(`
            LOCAL_DYNAMO environment variable detected but no local dynamoDB was
            found listening on port ${dynamoPort}. If you intend to talk to 'real'
            DynamoDB in AWS, please unset the environment variable. Otherwise
            ensure you have it running and if you have it listening to a port
            other than 8000 (the default) please ensure you are using the
            LOCAL_DYNAMO_PORT environment variable and try again.`)
        }
      }
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
