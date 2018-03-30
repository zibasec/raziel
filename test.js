const test = require('tape')
const Dynamo = require('.')

let db = null

test('sanity test', t => {
  t.ok(true)
  t.end()
})

test('setup', async t => {
  const { err, db: _db } = await Dynamo('test')
  db = _db
  t.ok(!err)
  t.end()
})

test('passing - put', async t => {
  const key = ['a', 'a']
  const value = { foo: 100 }

  const { err } = await db.put(key, value)

  t.ok(!err, err && err.message)
  t.end()
})

test('passing - get', async t => {
  const key = ['a', 'a']

  const { err, value } = await db.get(key)

  t.ok(!err, err && err.message)
  t.deepEqual(value, { foo: 100 }, 'object is the same')
  t.end()
})

test('failing - get', async t => {
  const key = []

  const { err } = await db.get(key)

  t.ok(err)
  t.end()
})

test('passing - del', async t => {
  const key = ['a', 'a']

  {
    const { err } = await db.del(key)
    t.ok(!err, err && err.message)
  }

  {
    const { err } = await db.get(key)
    t.ok(err)
    t.end()
  }
})

test('failing - del', async t => {
  const key = []

  const { err } = await db.get(key)

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

  const { err: errBatch } = await db.batch(ops)
  t.ok(!errBatch)
  t.end()
})

test('passing - query without prefix', async t => {
  const params = {
    key: ['a']
  }

  t.plan(5)

  const { events } = await db.query(params)
  t.ok(events)

  let count = 0

  events.on('data', d => {
    count++
    t.ok(true)
  })

  events.on('end', () => {
    t.equal(count, 3)
  })
})

test('passing - query with prefix', async t => {
  const params = {
    key: ['a', 'b']
  }

  t.plan(3)

  const { events } = await db.query(params)
  t.ok(events)

  let count = 0

  events.on('data', d => {
    count++
    t.ok(true)
  })

  events.on('end', () => {
    t.equal(count, 1)
  })
})
