const debug = require('debug')('raziel')
const AWS = require('aws-sdk')
const dateAt = require('date-at')

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

class Table {
  constructor (table, opts) {
    this.table = table
    this.opts = opts || {}
    this.db = opts.db
    this.waitFor = opts.waitFor || false
  }

  async then (resolve) {
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
      debug('table encryption enabled')
      params.SSESpecification = {
        Enabled: true
      }
    }

    if (this.opts.streaming) {
      debug('table streaming enabled')
      params.StreamSpecification = {
        StreamEnabled: true,
        StreamViewType: this.opts.streaming
      }
    }

    const table = this

    if (!this.opts.createIfNotExists) {
      debug('not attemtping to create, createIfNotExists not supplied')
      return resolve({ db: this.db, table })
    }

    try {
      debug('attempting to create table')
      await this.db.createTable(params).promise()
    } catch (err) {
      if (err.name !== 'ResourceInUseException') {
        debug('error creating table')
        return resolve({ err })
      }
    }

    if (this.waitFor) {
      debug('waitFor specified')
      try {
        const params = { TableName: this.table }
        await this.db.waitFor('tableExists', params).promise()
        let count
        while (true) {
          if (count > 10) { // should never happen unless there is an AWS issue
            debug('timeout waiting for ACTIVE STATE')
            resolve({ err: new Error('Timed out while waiting for ACTIVE state') })
          }
          await sleep(1500)
          const out = await this.db.describeTable(params).promise()
          if (out.Table.TableStatus === 'ACTIVE') {
            break
          }
          count++
        }
      } catch (err) {
        debug('error waiting for table to be active')
        resolve({ err })
      }
    }

    if (this.opts.ttl) {
      debug('ttl options specified')
      let enabled = false

      try {
        const params = { TableName: this.table }
        const data = await this.db.describeTimeToLive(params).promise()
        enabled = data &&
          data.TimeToLiveDescription &&
          data.TimeToLiveDescription.TimeToLiveStatus &&
          data.TimeToLiveDescription.TimeToLiveStatus === 'ENABLED'
      } catch (err) {
        debug('error describing TimeToLive')
        return resolve({ err })
      }

      if (!enabled) {
        debug('TTL no currently enabled. Enabling...')
        const params = {
          TableName: this.table,
          TimeToLiveSpecification: {
            AttributeName: 'ttl',
            Enabled: true
          }
        }

        try {
          await this.db.updateTimeToLive(params).promise()
          debug('ttl updated')
        } catch (err) {
          debug('error updating ttl')
          return resolve({ err })
        }
      }
    }
    resolve({ db: this.db, table })
  }

  async put (key, opts, value) {
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

    if (opts && opts.ttl) {
      params.Item.ttl = {
        N: String(dateAt(opts.ttl).getTime() / 1000)
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
      debug('batchGetItem err')
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
      debug('error getting item')
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
      debug('error deleting item')
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

    let key = opts.key.shift()
    let prefix = opts.key.join('/')

    if (prefix) {
      debug(`prefix => ${prefix}`)
      params.ExpressionAttributeValues = {
        ':key': { S: key },
        ':prefix': { S: prefix }
      }
      params.KeyConditionExpression = HASPREFIX
    } else if (key) {
      debug(`key => ${key}`)
      params.ExpressionAttributeValues = {
        ':key': { S: key }
      }
      params.KeyConditionExpression = 'hkey = :key'
    }

    if (opts.limit) {
      debug(`limit ${opts.limit}`)
      params.Limit = opts.limit
    }

    if (opts.start) {
      debug(`start ${opts.start}`)
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
        debug('scan error')
        return { err }
      }

      count += data.Count

      if (!data.LastEvaluatedKey) break
      params.ExclusiveStartKey = data.LastEvaluatedKey
      progress()
    }

    return { count }
  }

  async batch (ops, opts = {}) {
    const parseOp = op => {
      op = clone(op)

      if (op.type === 'put') {
        let v = null

        try {
          v = JSON.stringify(op.value)
        } catch (err) {
          return { err }
        }

        const o = {
          PutRequest: {
            Item: {
              hkey: { S: op.key.shift() },
              rkey: { S: op.key.join(this.opts.sep) },
              value: { S: v }
            }
          }
        }

        if (op.ttl) {
          o.PutRequest.Item.ttl = {
            N: String(dateAt(op.ttl).getTime() / 1000)
          }
        }

        return o
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
      debug('error with batch write')
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

  async open (table, opts = {}) {
    if (!table) {
      throw new Error('table name required')
    }

    if (process.env['LOCAL_DYNAMO']) {
      debug('local dynamo in use')
      const dynamoPort = process.env['LOCAL_DYNAMO_PORT'] || 8000
      opts.endpoint = `http://localhost:${dynamoPort}`
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
