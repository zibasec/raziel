const test = require('tape')
const AWS = require('aws-sdk')
const Database = require('.')

let db = null
let table = null

test('sanity test', t => {
  t.ok(true)
  t.end()
})

test('setup', async t => {
  const opts = {}

  if (process.env['PROFILE']) {
    opts.credentials = new AWS.SharedIniFileCredentials({
      profile: process.env['PROFILE']
    })
  }

  db = new Database(opts)
  t.ok(db.db, 'exposes the underlying database connection')

  const { err: errTable, table: _table } = await db.open('test')
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
    { type: 'put', key: ['a', 'c'], value: 0 },
    { type: 'put', key: ['b', 'a'], value: 0 }
  ]

  const { err: errBatch } = await table.batch(ops)
  t.ok(!errBatch)
  t.end()
})

test('passing - query without prefix', async t => {
  const params = {}

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
    key: ['a']
  }

  const iterator = table.query(params)
  t.ok(iterator, 'has an iterator')

  let count = 0

  while (true) {
    const { err, key, value, done } = await iterator.next()

    if (done) break
    count++

    console.log(key, value)
    t.ok(!err, err && err.message)
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

  while (true) {
    const { err, key, value, done } = await iterator.next()

    if (done) break
    count++

    console.log(key, value)
    t.ok(!err, err && err.message)
    t.notEqual(key, undefined, 'has a key')
    t.notEqual(value, undefined, 'has a value')
  }

  t.equal(count, 1)
  t.end()
})
