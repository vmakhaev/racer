{objEquiv} = require '../util'

MongoQueryBuilder = module.exports = ->
  @fields = {}
  return

MongoQueryBuilder:: =
  register: (field, config) ->
    @fields[field] = if config == true
      'direct'
    else
      config

  applyOps: (oplog, callback) ->
    oplog = @_minifyOps oplog
    queries = @_queriesForOps oplog
    remainingQueries = queries.length
    for {method, args} in queries
      # e.g., adapter.update {_id: id}, {$set: {name: 'Brian', age: '26'}}, {upsert: true, safe: true}, callback
      adapter[method] args..., (err) ->
        --remainingQueries || callback() if callback

  _compileQuery: (query) ->
    args = query.args = []
    opts = query.opts ||= {}
    opts.safe = opts.upsert = true
    args.push query.conds, query.val, opts
    return query

  _queriesForOps: (oplog) ->
    queries = []
    query = {}
    for op in oplog
      {conds, method, args} = operation.splat op
      [query, nextQuery] = @[method] query, conds, args...
      if nextQuery
        queries.push @_compileQuery query
        query = nextQuery

    if query
      queries.push @_compileQuery query

    return queries

  # e.g., accomplish optimizations such as collapsing
  # multiple sequental push ops into a single atomic push
  _minifyOps: (oplog) -> oplog

  # TODO Better lang via MongoQueryBuilder.handle 'set', (...) -> ?
  set: (query, conds, path, val, ver) ->
    nextQuery = null
    field = @fields[path]
    # Assign or augment query.(method|conds|val)
    {method: qmethod, conds: qconds} = query
    if qmethod is undefined && qconds is undefined
      query.method = 'update'
      query.conds = conds
      (delta = {})[path] = val
      query.val = { $set: delta }
    else if qmethod == 'update' && objEquiv qconds, conds
      # Better to have a pre-compiled, and post-compiled version of query
      # Pre-compiled will look like:
      #   query.method
      #   query.conds
      #   query.val
      #   query.opts
      # Post-compiled will look like:
      #   query.method
      #   query.args
      delta = query.val['$set'] ||= {}
      delta[path] = val
    else
      throw new Error "Unsuported"
    return [query, nextQuery]

  del: (query, path, ver) ->
    field = @fields[path]

  push: (query, path, values..., ver) ->
  pop: (query, path, values..., ver) ->

operation =
  splat: ([conds, method, args...]) -> {conds, method, args}
