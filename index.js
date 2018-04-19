const AWS = require('aws-sdk')

const NOTEXISTS = 'attribute_not_exists(hkey) AND attribute_not_exists(rkey)'
const HASPREFIX = 'hkey = :key and begins_with(rkey, :prefix)'

let createdTable = process.env['NODE_ENV'] === 'production'

const ERR_KEY_LEN = new Error('Malformed key, expected [hash, range, ...]')
const ERR_KEY_TYPE = new Error('Expected an array')
const ERR_KEY_EMPTY = new Error('Hash or Range can not be empty')

const assertKey = (key, resolve) => {
  if (!Array.isArray(key)) {
    resolve({ err: ERR_KEY_TYPE, key })
    return false
  }

  if (!(key.length >= 2)) {
    resolve({ err: ERR_KEY_LEN, key })
    return false
  }

  if (!key[0] || !key[1]) {
    resolve({ err: ERR_KEY_EMPTY, key })
    return false
  }

  return true
}

class Table {
  constructor (table, opts) {
    this.table = table
    this.opts = opts
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

    const table = this

    if (this.opts.assumeExists || createdTable) {
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

  put (key, opts = {}, value) {
    return new Promise(resolve => {
      if (!value) {
        value = opts
        opts = null
      }

      if (!assertKey(key, resolve)) return

      const k = key.slice()
      let v = null

      try {
        v = JSON.stringify(value)
      } catch (err) {
        return resolve({ err })
      }

      const params = {
        Item: {
          hkey: { S: k.shift() },
          rkey: { S: k.join(this.sep) },
          value: { S: v }
        },
        TableName: (opts && opts.table) || this.table
      }

      if (opts && opts.notExists) {
        params.ConditionExpression = NOTEXISTS
      }

      this.db.putItem(params, (err) => {
        if (err) return resolve({ err })
        resolve({})
      })
    })
  }

  get (key, opts = {}) {
    return new Promise(resolve => {
      const k = key.slice()

      if (!assertKey(key, resolve)) return

      const params = {
        TableName: (opts && opts.table) || this.table,
        Key: {
          hkey: { S: k.shift() },
          rkey: { S: k.join(this.sep) }
        }
      }

      this.db.getItem(params, (err, data) => {
        if (err) return resolve({ err })

        let value = null

        try {
          value = JSON.parse(data.Item.value.S)
        } catch (err) {
          return resolve({ err })
        }

        resolve({ value })
      })
    })
  }

  del (key, opts = {}) {
    return new Promise(resolve => {
      if (!assertKey(key, resolve)) return

      const k = key.slice()
      const params = {
        Key: {
          hkey: { S: k.shift() },
          rkey: { S: k.join(this.sep) }
        },
        TableName: (opts && opts.table) || this.table
      }

      this.db.deleteItem(params, (err) => {
        if (err) return resolve({ err })
        resolve({})
      })
    })
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

    return {
      next: () => ({
        then (resolve) {
          if (i < array.length) {
            const data = array[i++]

            return resolve({
              key: data.key,
              value: data.value,
              done: false
            })
          }

          if (complete) {
            return resolve({ done: true })
          }

          db[method](params, (err, response) => {
            if (err) {
              return resolve({ err })
            }

            if (!response || !response.Items) {
              return resolve({ done: true })
            }

            response.Items.map(item => {
              let value = null

              try {
                value = JSON.parse(item.value.S)
              } catch (err) {
                return resolve({ err })
              }

              const key = [item.hkey.S, item.rkey.S]
              array.push({ key, value })
            })

            if (typeof response.LastEvaluatedKey === 'undefined') {
              complete = true
            }

            const data = array[i++]

            resolve({
              key: data.key,
              value: data.value,
              done: false
            })
          })
        }
      })
    }
  }

  batch (ops, opts = {}) {
    return new Promise(resolve => {
      const parseOp = op => {
        if (op.type === 'put') {
          let v = null

          try {
            v = JSON.stringify(op.value)
          } catch (err) {
            return resolve({ err })
          }

          return {
            PutRequest: {
              Item: {
                hkey: { S: op.key.shift() },
                rkey: { S: op.key.join(this.sep) },
                value: { S: v }
              }
            }
          }
        }

        return {
          DeleteRequest: {
            Key: {
              hkey: { S: op.key.shift() },
              rkey: { S: op.key.join(this.sep) }
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

      this.db.batchWriteItem(params, (err) => {
        if (err) return resolve({ err })
        return resolve({})
      })
    })
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
