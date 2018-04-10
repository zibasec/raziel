# SYNOPSIS
A light weight, async/await abstraction for DynamoDB.


# BUILD STATUS
[![CircleCI](https://circleci.com/gh/MindPointGroup/raziel/tree/master.svg?style=svg&circle-token=5bd6211fdb6cbe6df549b89b9f3d478f767e0d0d)](https://circleci.com/gh/MindPointGroup/raziel/tree/master)


# USAGE
```bash
npm install MindPointGroup/raziel
```

## CONSTRUCTOR
Create a constructor and configure it.

```js
const Database = require('raziel')
const db = new Database({ region: 'us-east-1' })
```

## TABLES
Create or open a table.

```js
const { err, table } = await db.open('imports')
```

## PUT
Put a key/value.

```js
const { err } = await table.put(['a', 'b'], { foo: 100 })
```

A key is input as an array. The first item in the array is the
`partition` and the rest of the items are eventually concatenated
to form the `range`. The `partition` is like a `grouping` of keys.
Keys are sorted lexicographically and can be queried or looked up.


## GET
Get a key/value

```js
const { err, value } = await table.get(['a', 'b'])
assert(value === { foo: 100 })
```

## DEL
Delete a key/value

```js
const { err } = await table.del(['a', 'b'])
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
const { err } = await table.batch(ops)
```

## QUERY
Get a range of keys and their values. This produces an
[iterator][0] with a next method that is awaitable.

If the previous batch command was executed, there there were
only three records in the database. Two events would be
emitted by the following query.

```js
const iterator = table.query({ key: ['a'] })

while (true) {
  const { err, key, value, done } = await iterator.next()

  if (done) break // only true when there is no more data
}
```

[0]:https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Iteration_protocols
