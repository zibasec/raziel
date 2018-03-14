# SYNOPSIS
A light weight, async/await abstraction for DynamoDB.

# USAGE

## OPEN
Create or open a table.

```js
const Dynamo = require('../db')
const { err, db } = await Dynamo('imports')
```

## PUT
Put a key/value.

```js
const { err } = await db.put(['a', 'b'], { foo: 100 })
```

A key is input as an array. The first item in the array is the
`partition` and the rest of the items are eventually concatenated
to form the `range`. The `partition` is like a `grouping` of keys.
Keys are sorted lexicographically and can be queried or looked up.


## GET
Get a key/value

```js
const { err, value } = await db.get(['a', 'b'])
assert(value === { foo: 100 })
```

## DEL
Delete a key/value

```js
const { err } = await db.del(['a', 'b'])
```

## BATCH
Put and or delete multiple key/values

```js
const ops = [
  { type: 'put', key: ['a', 'a'], value: { foo: 100 } },
  { type: 'put', key: ['a', 'b'], value: { bar: 200 } },
  { type: 'put', key: ['b', 'a'], value: { baz: 300 } },
  { type: 'del', key: ['a', 'c'] }
]
const { err } = await db.batch(ops)
```

## QUERY
Get a range of keys and their values. This produces an
event emitter that emits a `data` event when it finds a
new key/value pair. The `end` event is emitted when there
are no more records found. If there is an error, it will
bubble up from the aws-sdk and no more events will be
emitted.

If the previous batch command was executed, there there were
only three records in the database. Two events would be
emitted by the following query.

```js
const { events } = await db.query({ key: ['a'] })

events.on('data', ({ key, value }) => {
  assert(key === ['a', 'a'])
  assert(value === { foo: 100 })
})

events.once('error', err => {
  // ...the dynamodb error
})

events.once('end', () => {
  // ...done!
})
```
