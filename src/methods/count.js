const api = module.exports = {}

api.count = async function count (opts, progress) {
  const params = {
    TableName: (opts && opts.table) || this.table,
    Select: 'COUNT'
  }

  let count = 0

  while (true) {
    let data = null

    try {
      data = await this.db.scan(params).promise()
    } catch (err) {
      return { err }
    }

    count += data.Count

    if (!data.LastEvaluatedKey) break
    params.ExclusiveStartKey = data.LastEvaluatedKey
    progress()
  }

  return { count }
}
