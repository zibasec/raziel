# SYNOPSIS
A light weight, async/await abstraction for DynamoDB.


# USAGE

#### AS A LIBRARY

```bash
npm install mindpointgroup/raziel
```

#### AS A CLI

```bash
npm install mindpointgroup/raziel -g
```

## CONSTRUCTOR
Create a constructor and configure it.

```js
const Database = require('dynamodb')
const db = new Database({ region: 'us-west-2' })
```

## TABLES
Create or open a table. The optional options object may include...

```js
const options = {
  encrypted: true || false,
  streaming: 'NEW_IMAGE' || 'OLD_IMAGE' || 'NEW_AND_OLD_IMAGES' || 'KEYS_ONLY',
  createIfNotExists: true || false,
  ttl: true || false
}

const { err, table } = await db.open('foo', options)
```

## PUT
Put a key/value.

```js
const { err } = await table.put(['a', 'a'], { foo: 100 })
```

A key is input as an array. The first item in the array is the
`partition` and the rest of the items are eventually concatenated
to form the `range`. The `partition` is like a `grouping` of keys.
Keys are sorted lexicographically and can be queried or looked up.

You can optionally provide more arguments that will be used as
attributes.

```js
const attr1 = { test: { S: 'Foo' } }
const { err } = await table.put(['a', 'a'], value, attr1)
```

#### TTL
An object literal can be provided as the second positional argument.
This is where a `TTL` (time to live) value can be specified. When
specified, the item will be deleted from the database if the duration
specified is less than the current time.

For example the following `put` operation will add an item to the
database which will be removed after `1 second`.

```js
const { err } = await table.put(['a', 'a'], { ttl: '+1s' }, { foo: 100 })
```

See [this](https://github.com/hxoht/date-at) repo for ttl units of measure.

## GET
Get a key/value

```js
const { err, value } = await table.get(['a', 'b'])
assert(value === { foo: 100 })
```

## GET MULTIPLE VALUES AT ONCE
Specify an array of keys as the first argument to the `get` method.

If a requested item does not exist, it is not returned in the result.
Requests for nonexistent items consume the minimum read capacity units
according to the type of read (this is how the Dynamodb aws-sdk works).

Gets are limited to 100 keys at a time (specified in the aws api).

```js
const { err, data } = await table.get([['a', 'a'], ['a', 'b']])

assert.deepEqual(data[0].key, ['a', 'a'])
assert.deepEqual(data[0].value, { foo: 100 })
```

## DEL
Delete a key/value

```js
const { err } = await table.del(['a', 'b'])
```

## BATCH
Put and or delete multiple key/values. Use an array of objects. With `type` and
`key`. If the `type` is `put`, you must specify `value` and you can also provide
other attributes (like the `put` method).

```js
const ops = [
  { type: 'put', key: ['a', 'a'], value: { foo: 100 } },
  { type: 'put', key: ['a', 'b'], value: { bar: 200 } },
  { type: 'put', key: ['b', 'a'], value: { baz: 300 }, someOtherAttribute: 1 },
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
const params = {
  key: ['a']
}

const iterator = table.query(params)

for await (const { key, value } of iterator) {
  console.log(key, value)
}
```

### ADVANCED QUERIES

```js
const iterator = table.query(
  `hkey = "z" AND begins_with(rkey, "a")`,
  `x = "b"`
)
```

In the above example, the first argument is the [`KeyConditionExpression`][ke],
and the second argument is the [`FilterExpression`][fe]. You don't need to
specify the [`ExpressionAttributeValues`][eav] since thats already parsed out
for you. Values are contained in double quotes and you can mitigate reserved
words using an octothorpe (`#`).

### LEGACY NODE VERSIONS

In node versions less than 10.1.x
```js
const iterator = table.query({ key: ['a'] })

while (true) {
  const { err, key, value, done } = await iterator.next()

  if (done) break // only true when there is no more data
}
```

## Working with local DynamoDB
If you would like to use [DynamoDB Local][1] you will want to set the following
environment variables.

```
LOCAL_DYNAMO=true
LOCAL_DYNAMO_PORT=8000
```

`LOCAL_DYNAMO_PORT` is optional and defaults to 8000 which is the default port.

**NOTE:** This does not validate that a proper DynamoDB process is listenting on
`8000` (or specified port).


# Testing

Local Testing

```
npm run test
```

CircleCI Emulator ([require the circleci CLI to be installed][2])

```
circleci local execute --job node-lambda -e AWS_ACCESS_KEY_ID=ABCDEF -e AWS_SECRET_ACCESS_KEY=GHIJKL
circleci local execute --job node-v8 -e AWS_ACCESS_KEY_ID=ABCDEF -e AWS_SECRET_ACCESS_KEY=GHIJKL
circleci local execute --job node-v10 -e AWS_ACCESS_KEY_ID=ABCDEF -e AWS_SECRET_ACCESS_KEY=GHIJKL
```

[0]:https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Iteration_protocols
[1]:https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html
[2]:https://github.com/CircleCI-Public/circleci-cli

[ke]:https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#DDB-Query-request-KeyConditionExpression
[fe]:https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#DDB-Query-request-FilterExpression
[eva]:https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#DDB-Query-request-ExpressionAttributeValues) or the [`ExpressionAttributeNames`](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#DDB-Query-request-ExpressionAttributeNames
