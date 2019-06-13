const test = require('tape')
const Database = require('../src')
const AWS = require('aws-sdk')

let db = null
let table = null
let encTable = null

const sleep = t => new Promise(resolve => setTimeout(resolve, t))

const cleanup = async () => {
  const testTables = ['raziel_test', 'raziel_test_encrypted']
  const dynamo = new AWS.DynamoDB()

  const delTable = async TableName => {
    try {
      await dynamo.deleteTable({ TableName }).promise()
      await dynamo.waitFor('tableNotExists', { TableName }).promise()
    } catch (err) {
      // db doesnt exist, so we don't care
      if (err.code !== 'ResourceNotFoundException') {
        throw err
      }
    }
  }
  const promises = testTables.map(delTable)
  await Promise.all(promises)
}

test.onFinish(cleanup)
test.onFailure(cleanup)

test('setup', async t => {
  try {
    await cleanup()
  } catch (err) {
    t.fail(err)
  }

  const opts = {}

  db = new Database(opts)
  t.ok(db.db, 'exposes the underlying database connection')

  const params = {
    waitFor: true,
    createIfNotExists: true,
    ttl: true
  }

  const { err: errTable, table: _table } = await db.open('raziel_test', params)
  t.ok(!errTable, errTable && errTable.message)

  table = _table
  t.end()
})

test('passing - put', async t => {
  const key = ['a', 'a']
  const value = { foo: 100 }

  const { err } = await table.put(key, value)

  t.ok(!err, err && err.message)
  t.end()
})

test('passing - get', async t => {
  const key = ['a', 'a']

  const { err, value } = await table.get(key)

  t.ok(!err, err && err.message)
  t.deepEqual(value, { foo: 100 }, 'object is the same')
  t.end()
})

test('failing - get', async t => {
  const key = []

  const { err } = await table.get(key)

  t.ok(err)
  t.end()
})

test('passing - del', async t => {
  const key = ['a', 'a']

  {
    const { err } = await table.del(key)
    t.ok(!err, err && err.message)
  }

  {
    const { err } = await table.get(key)
    t.ok(err)
    t.end()
  }
})

test('failing - del', async t => {
  const key = []

  const { err } = await table.get(key)

  t.ok(err)
  t.end()
})

test('passing - batch', async t => {
  const ops = [
    { type: 'put', key: ['a', 'a'], value: 0 },
    { type: 'put', key: ['a', 'b'], value: 0 },
    { type: 'put', key: ['a', 'c'], value: 100 },
    { type: 'put', key: ['b', 'a'], value: 100 }
  ]

  const { err: errBatch } = await table.batch(ops)
  t.ok(!errBatch)
  t.end()
})

test('passing - multiget', async t => {
  const keys = [
    ['a', 'a'],
    ['a', 'b'],
    ['a', 'c'],
    ['b', 'a']
  ]

  const { err, data } = await table.get(keys)
  t.ok(!err, err && err.message)
  t.ok(data)
  t.ok(Array.isArray(data))
  t.equal(data.length, 4)
  t.end()
})

test('passing - multiget with holes', async t => {
  const keys = [
    ['a', 'a'],
    ['a', 'x'],
    ['b', 'a']
  ]

  const { err, data } = await table.get(keys)
  t.ok(!err, err && err.message)
  t.ok(data)
  t.ok(Array.isArray(data))
  t.equal(data.length, 2)
  t.end()
})

test('passing - put additional attributes', async t => {
  await table.put(['z', 'aa'], null, { foo: 'bar' }, { x: { S: 'a' } })
  await table.put(['z', 'ab'], null, { foo: 'bar' }, { x: { S: 'b' } })
  await table.put(['z', 'ac'], null, { foo: 'bar' }, { x: { S: 'c' } })
  await table.put(['z', 'ad'], null, { foo: 'bar' }, { x: { S: 'd' } })

  {
    const { err, value, x } = await table.get(['z', 'ab'])
    t.equal(x.S, 'b', 'attribute found')
    t.ok(!err, 'key/value was added')
    t.deepEqual(value, { foo: 'bar' })
  }

  {
    const itr = table.query(
      `hkey = "z" AND rkey BETWEEN "aa" AND "ac"`
    )

    let keys = []

    for await (const { key } of itr) keys.push(key)

    t.equal(keys.length, 3, 'correct number of records received from query')
  }

  {
    const itr = table.query(
      `hkey = "z" AND begins_with(rkey, "a")`
    )

    let keys = []

    for await (const record of itr) {
      keys.push(record.key)
      t.ok(record.x.S, 'attribute found')
    }

    t.equal(keys.length, 4, 'correct number of records received from query')
  }

  {
    const itr = table.query(
      `hkey = "z" AND begins_with(rkey, "a")`,
      `x = "b"`
    )

    let keys = []

    for await (const { key } of itr) keys.push(key)

    t.equal(keys.length, 1, 'correct number of records received from query')
  }

  t.end()
})

test('passing - enable encryption on table', async t => {
  const { err: errTable, table: _table } = await db.open('raziel_test_encrypted', { waitFor: true, encrypted: true, createIfNotExists: true })
  t.ok(!errTable, errTable && errTable.message)
  encTable = _table
  t.ok(encTable)
  const { err: errPut } = await encTable.put(['a', 'a'], { foo: 100 })
  t.ok(!errPut, errPut && errPut.message)
  t.end()
})

test('failing - multiget without keys', async t => {
  const keys = []

  const { err } = await encTable.get(keys)
  t.ok(err, err && err.message)
  t.end()
})

test('passing - query without prefix', async t => {
  const params = {}

  const iterator = table.query(params)
  t.ok(iterator, 'has an iterator')

  let count = 0

  for await (const { key, value } of iterator) {
    count++

    t.notEqual(key, undefined, 'has a key')
    t.notEqual(value, undefined, 'has a value')
  }

  t.equal(count, 8)
  t.end()
})

test('passing - query with limit', async t => {
  const p = { limit: 3 }

  const iterator = table.query(p)

  let count = 0

  for await (const { key, value } of iterator) {
    count++

    t.notEqual(key, undefined, 'has a key')
    t.notEqual(value, undefined, 'has a value')
  }

  t.equal(count, 3)
  t.end()
})

test('passing - query with limit', async t => {
  const p = { limit: 3 }

  const iterator = table.query(p)

  let count = 0

  for await (const { key, value } of iterator) {
    count++

    t.notEqual(key, undefined, 'has a key')
    t.notEqual(value, undefined, 'has a value')
  }

  t.equal(count, 3)
  t.end()
})

test('passing - query with hash component', async t => {
  const params = {
    key: ['a']
  }

  const iterator = table.query(params)
  t.ok(iterator, 'has an iterator')

  let count = 0

  for await (const { key, value } of iterator) {
    count++

    t.notEqual(key, undefined, 'has a key')
    t.notEqual(value, undefined, 'has a value')
  }

  t.equal(count, 3)
  t.end()
})

test('passing - query with hash and range components', async t => {
  const params = {
    key: ['a', 'b']
  }

  const iterator = table.query(params)
  t.ok(iterator, 'has an iterator')

  let count = 0

  for await (const { key, value } of iterator) {
    count++

    t.notEqual(key, undefined, 'has a key')
    t.notEqual(value, undefined, 'has a value')
  }

  t.equal(count, 1)
  t.end()
})

test('passing - async iterator', async t => {
  const params = {
    key: ['a']
  }

  const it = table.query(params)
  t.ok(it, 'has an iterator')
  let count = 0

  for await (const { key, value } of it) {
    count++
    t.notEqual(key, undefined, 'has a key')
    t.notEqual(value, undefined, 'has a value')
  }

  t.equal(count, 3)
  t.end()
})

test('passing - count', async t => {
  const { err, count } = await table.count()
  t.ok(!err, err && err.message)
  t.equal(count, 8, 'count is correct')
  t.end()
})

//
// This can not be tested. Dynamo doesn't delete the keys immediately, only
// within 48 hours. So instead we build in some logic to check a ttl when doing
// reads.
//
// https://amzn.to/2NuE11q
//
test('passing - ttl', async t => {
  const key = ['a', 'a']
  const expected = { deletable: true }

  {
    const { err } = await table.put(key, { ttl: '+4s' }, expected)
    t.ok(!err, err && err.message)
  }

  {
    const { err } = await table.get(key)
    t.ok(!err, 'key/value was added')
  }

  {
    await sleep(1e4)
    const { err } = await table.get(key)
    t.ok(err, 'there was an expected error')
    t.ok(err.notFound, 'key/value was removed')
  }

  t.end()
})

test('passing - ttl via batch a operation', async t => {
  const key = ['a', 'a']
  const expected = { deletable: true }

  const batch = [
    {
      type: 'put',
      key,
      ttl: '+4s',
      value: expected
    }
  ]

  {
    const { err } = await table.batch(batch)
    t.ok(!err, err && err.message)
  }

  {
    const { err } = await table.get(key)
    t.ok(!err, 'key/value was added')
  }

  {
    await sleep(1e4)
    const { err } = await table.get(key)
    t.ok(err, 'there was an expected error')
    t.ok(err.notFound, 'key/value was removed')
  }

  t.end()
})
