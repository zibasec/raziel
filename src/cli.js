const minimist = require('minimist')
const Database = require('.')
const pkg = require('../package.json')
const stringify = require('json-stringify-safe')

const argv = minimist(process.argv.slice(2))

const db = new Database({
  region: argv.region || 'us-east-1'
})

if (argv.h || argv.help || !argv._.length) {
  const help = `
  Usage:
    raziel put <table> <hkey/rkey> <value> [--ttl <n>]
    raziel get <table> <hkey[/rkey]> [--limit <n>]
    raziel del <table> <hkey[/rkey]> [--limit <n>]
    raziel del <table>
    raziel count <table>
    raziel [-v][--version]
  `

  console.log(help)
  process.exit(0)
}

if (argv.v || argv.version) {
  console.log(pkg.version)
  process.exit(0)
}

async function main () {
  const command = argv._[0]
  const tableName = argv._[1]

  const { err: errTable, table } = await db.open(tableName, {})

  if (errTable) {
    console.log(errTable.message)
    process.exit(1)
  }

  if (command === 'put') {
    const key = argv._[2].split('/')
    const value = argv._[3]
    const { err } = await table.put(key, { ttl: argv.ttl }, value)

    if (err) {
      console.log(err)
      process.exit(1)
    }
    process.exit(0)
  }

  if (command === 'get') {
    const key = argv._[2] ? argv._[2].split('/') : []
    const it = table.query({ key })
    let count = 0

    for await (const { key, value } of it) {
      if (argv.limit && (count++ === argv.limit)) break
      process.stdout.write(stringify({ key, value }) + '\n')
    }

    process.exit(0)
  }

  if (command === 'count') {
    process.stdout.write('Please wait...')
    const { err, count } = await table.count(null, () => {
      process.stdout.write('.')
    })

    if (err) {
      console.log(err)
      process.exit(1)
    }

    process.stdout.write(`${count}\n`)

    process.exit(0)
  }

  const batch = []
  let deleted = 0
  let params = {
    key: []
  }

  if (argv._[2]) {
    params = {
      key: argv._[2].split('/')
    }
  }

  const it = table.query(params)

  if (command === 'del') {
    for await (const { key } of it) {
      if (batch.length === 25) {
        const { err } = await table.batch(batch)

        if (err) {
          console.log(err.message)
          process.exit(1)
        }

        batch.length = 0
        deleted += 25
      }

      batch.push({ type: 'del', key })
    }

    if (batch.length) {
      const { err } = await table.batch(batch)
      if (err) {
        console.log(err)
        process.exit(1)
      }
      deleted += batch.length
    }

    console.log(`Deleted ${deleted} rows`)
    process.exit(0)
  }
}

main()
