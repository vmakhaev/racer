MongoQueryBuilder = module.exports = ->
  @fields = {}
  return

MongoQueryBuilder:: =
  register: (field, config) ->
    @fields[field] = if config == true
      fields[field] = 'direct'
    else
      fields[field] = config

  applyOps: (oplog, callback) ->
    oplog = @_minifyOps oplog
    queries = []
    query = {}
    for op in oplog
      {conds, method, path, args} = operation.splat op
      [query, nextQuery] = @[method] query, path, args...
      if nextQuery
        queries.push query
        query = nextQuery
    remainingQueries = queries.length
    for {method, args} in queries
      # e.g., adapter.update {_id: id}, {$set: {name: 'Brian', age: '26'}}, {upsert: true, safe: true}, callback
      adapter[method] args..., (err) ->
        --remainingQueries || callback() if callback

  # e.g., accomplish optimizations such as collapsing
  # multiple sequental push ops into a single atomic push
  _minifyOps: (oplog) -> oplog

  # TODO Better lang via MongoQueryBuilder.handle 'set', (...) -> ?
  set: (query, conds, path, val, ver) ->
    field = @fields[path]
    if query.method is undefined
      query.method = 'update'
      query.args = []
      delta = {}
      delta[path] = val
      query.args.push conds, { $set: delta }, {upsert: true, safe: true}
    return [query]

  del: (query, path, ver) ->
    field = @fields[path]

  push: (query, path, values..., ver) ->
  pop: (query, path, values..., ver) ->

operation =
  splat: ([conds, method, path, args]) -> {conds, method, path, args}
