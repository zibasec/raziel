const AWS = require('aws-sdk')
const { EventEmitter } = require('events')

if (process.platform === 'darwin') {
  const credentials = new AWS.SharedIniFileCredentials({ profile: 'mpg' })
  AWS.config.credentials = credentials
}

const NOTEXISTS = 'attribute_not_exists(hkey) AND attribute_not_exists(rkey)'
const db = new AWS.DynamoDB(require('../config.json'))
let createdTable = process.env['NODE_ENV'] === 'production'

module.exports = async (table, opts) => ({
  then (done) {
    opts = opts || {}

    const sep = opts.sep || '/'
    const api = {}

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
      TableName: table
    }

    api.put = (key, opts, value) => ({
      then (done) {
        if (!value) {
          value = opts
          opts = null
        }

        const k = key.slice()
        let v = null

        try {
          v = JSON.stringify(value)
        } catch (err) {
          return done({ err })
        }

        const params = {
          Item: {
            hkey: { S: k.shift() },
            rkey: { S: k.join(sep) },
            value: { S: v }
          },
          TableName: table
        }

        if (opts && opts.notExists) {
          params.ConditionExpression = NOTEXISTS
        }

        db.putItem(params, (err, data) => {
          if (err) return done({ err })
          done({})
        })
      }
    })

    api.get = (key) => ({
      then (done) {
        const k = key.slice()

        const params = {
          TableName: table,
          Key: {
            hkey: { S: k.shift() },
            rkey: { S: k.join(sep) }
          }
        }
        db.getItem(params, (err) => {
          if (err) return done({ err })
          done({})
        })
      }
    })

    api.del = (key) => ({
      then (done) {
        const k = key.slice()
        const params = {
          Key: {
            hkey: { S: k.shift() },
            rkey: { S: k.join(sep) }
          },
          TableName: table
        }

        db.deleteItem(params, (err) => {
          if (err) return done({ err })
          done({})
        })
      }
    })

    api.query = (opts) => ({
      then (done) {
        const params = {
          TableName: table,
          ProjectionExpression: 'hkey, rkey, #val',
          ExpressionAttributeNames: {
            '#val': 'value'
          }
        }

        const key = opts.key.shift()
        const prefix = opts.key.join('/')

        if (prefix) {
          params.ExpressionAttributeValues = {
            ':key': { S: key },
            ':prefix': { S: prefix }
          }
          params.KeyConditionExpression = 'hkey = :key and begins_with(rkey, :prefix)'
        } else {
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

        const events = new EventEmitter()

        function query () {
          db.query(params, (err, data) => {
            if (err) return events.emit('error', err)

            if (data && data.Items) {
              data.Items.map(item => {
                events.emit('data', item)
              })
            }

            if (typeof data.LastEvaluatedKey !== 'undefined') {
              params.ExclusiveStartKey = data.LastEvaluatedKey
              return query()
            } else {
              events.emit('end')
            }
          })
        }

        query()

        done({ events })
      }
    })

    api.batch = (ops) => ({
      then (done) {
        const parseOp = op => {
          if (op.type === 'put') {
            let v = null

            try {
              v = JSON.stringify(op.value)
            } catch (err) {
              return done({ err })
            }

            return {
              PutRequest: {
                Item: {
                  hkey: { S: op.key.shift() },
                  rkey: { S: op.key.join(sep) },
                  value: { S: v }
                }
              }
            }
          }

          return {
            DeleteRequest: {
              Key: {
                hkey: { S: op.key.shift() },
                rkey: { S: op.key.join(sep) }
              }
            }
          }
        }

        const params = {
          RequestItems: {
            [table]: ops.map(parseOp)
          }
        }

        db.batchWriteItem(params, (err) => {
          if (err) return done({ err })
          return done({})
        })
      }
    })

    if (opts.assumeExists || createdTable) {
      return done({ db: api })
    }

    db.createTable(params, (err, data) => {
      if (err && err.name !== 'ResourceInUseException') {
        return done({ err })
      }

      createdTable = true

      done({ db: api })
    })
  }
})
