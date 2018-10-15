const test = require('tape')
const AWS = require('aws-sdk')
const Database = require('.')

let db = null
let table = null

const sleep = t => new Promise(resolve => setTimeout(resolve, t))

test('setup', async t => {
  const opts = {}

  db = new Database(opts)
  t.ok(db.db, 'exposes the underlying database connection')

  const params = {
    waitFor: true,
    createIfNotExists: true,
    ttl: true
  }

  const { err: errTable, table: _table } = await db.open('test', params)
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

test('passing - enable encryption on table', async t => {
  const { err: errTable, table } = await db.open('test_encrypted', { waitFor: true, encrypted: true, createIfNotExists: true })
  t.ok(!errTable, errTable && errTable.message)
  t.ok(table)
  const { err: errPut } = await table.put(['a', 'a'], { foo: 100 })
  t.ok(!errPut, errPut && errPut.message)
  t.end()
})

test('failing - multiget without keys', async t => {
  const keys = []

  const { err } = await table.get(keys)
  t.ok(err, err && err.message)
  t.end()
})

test('passing - query without prefix', async t => {
  const params = { legacy: true }

  const iterator = table.query(params)
  t.ok(iterator, 'has an iterator')

  let count = 0

  while (true) {
    const { err, key, value, done } = await iterator.next()

    if (done) break
    count++

    t.ok(!err, err && err.message)
    t.notEqual(key, undefined, 'has a key')
    t.notEqual(value, undefined, 'has a value')
  }

  t.equal(count, 4)
  t.end()
})

test('passing - query with hash component', async t => {
  const params = {
    key: ['a'],
    legacy: true
  }

  const iterator = table.query(params)
  t.ok(iterator, 'has an iterator')

  let count = 0

  while (true) {
    const { err, key, value, done } = await iterator.next()

    if (done) break
    count++

    t.ok(!err, err && err.message)
    t.notEqual(key, undefined, 'has a key')
    t.notEqual(value, undefined, 'has a value')
  }

  t.equal(count, 3)
  t.end()
})

test('passing - query with hash and range components', async t => {
  const params = {
    key: ['a', 'b'],
    legacy: true
  }

  const iterator = table.query(params)
  t.ok(iterator, 'has an iterator')

  let count = 0

  while (true) {
    const { err, key, value, done } = await iterator.next()

    if (done) break
    count++

    t.ok(!err, err && err.message)
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

  // eslint-disable-next-line no-alert
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
  t.equal(count, 4, 'count is correct')
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
