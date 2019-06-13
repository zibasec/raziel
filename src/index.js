const Debug = require('debug')
const AWS = require('aws-sdk')
const { sleep } = require('./util')

const debug = Debug('raziel')

const methods = {
  ...require('./methods/batch'),
  ...require('./methods/count'),
  ...require('./methods/del'),
  ...require('./methods/get'),
  ...require('./methods/put'),
  ...require('./methods/query')
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

    //
    // Structure for hash and range keys
    //
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
        ReadCapacityUnits: this.opts.readCapacity || 5,
        WriteCapacityUnits: this.opts.writeCapacity || 5
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

  uuid () {
    const filler = () => 'abcdef'[Math.floor(Math.random() * 6)]
    return Array.from({ length: 8 }, filler).join('')
  }
}

Object.assign(Table.prototype, methods)

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

  uuid () {
    return Math.random().toString(16).slice(2)
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
