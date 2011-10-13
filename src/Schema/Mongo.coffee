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

  # Better to build a query out of multiple ops using
  # a pre-compiled form; then post-compile the query for
  # use by the adapter once the query is done being built.
  # Pre-compiled will look like:
  #   query.method
  #   query.conds
  #   query.val
  #   query.opts
  # Post-compiled will look like:
  #   query.method
  #   query.args = [query.conds, query.val, query.opts]
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
    field = @fields[path]
    # Assign or augment query.(method|conds|val)
    {method: qmethod, conds: qconds} = query
    if qmethod is undefined && qconds is undefined
      query.method = 'update'
      query.conds = conds
      (delta = {})[path] = val
      query.val = { $set: delta }
    else if qmethod == 'update'
      if query.val.$set && objEquiv qconds, conds
        delta = query.val.$set ||= {}
        delta[path] = val
      else
        nextQuery = {}
        [nextQuery] = @set nextQuery, conds, path, val, ver
    else
      nextQuery = {}
      [nextQuery] = @set nextQuery, conds, path, val, ver
    return [query, nextQuery]

  del: (query, path, ver) ->
    field = @fields[path]

  push: (query, conds, path, values..., ver) ->
    unless values.length
      values.push ver
      ver = null
    # Assign or augment query.(method|conds|val)
    {method: qmethod, conds: qconds} = query
    if qmethod is undefined && qconds is undefined
      query.method = 'update'
      query.conds = conds
      if values.length == 1
        val = values[0]
        k = '$push'
      else if values.length > 1
        val = values
        k = '$pushAll'
      else
        throw new Error "length of 0! Uh oh!"

      val = if values.length then values[0] else values
      # TODO Only one field can be pushed at a time
      (args = {})[path] = val
      (query.val ||= {})[k] = args
    else if qmethod == 'update'
      if query.val.$push && objEquiv qconds, conds
        delta = query.val.$push ||= {}
        delta[path] = val
      else
        nextQuery = {}
        [nextQuery] = @set nextQuery, conds, path, val, ver
    else
      nextQuery = {}
      [nextQuery] = @set nextQuery, conds, path, val, ver
    return [query, nextQuery]

  pop: (query, path, values..., ver) ->

operation =
  splat: ([conds, method, args...]) -> {conds, method, args}
