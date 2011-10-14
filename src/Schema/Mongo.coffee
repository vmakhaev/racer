{objEquiv} = require '../util'

MongoQueryBuilder = module.exports = ->
  @fields = {}
  return

MongoQueryBuilder:: =
  addField: (field, config) ->
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
    # Assign or augment query.(method|conds|val)
    {method: qmethod, conds: qconds} = query
    if qmethod is undefined && qconds is undefined
      query.method = 'update'
      query.conds = conds
      (delta = {})[path] = val
      query.val = { $set: delta }
    else if qmethod == 'update'
      if (delta = query.val.$set) && objEquiv qconds, conds
        delta[path] = val
      else
        nextQuery = {}
        [nextQuery] = @set nextQuery, conds, path, val, ver
    else
      nextQuery = {}
      [nextQuery] = @set nextQuery, conds, path, val, ver
    return [query, nextQuery]

  del: (query, conds, path) ->
    # Assign or augment query.(method|conds|val)
    {method: qmethod, conds: qconds} = query
    if qmethod is undefined && qconds is undefined
      query.method = 'update'
      query.conds = conds
      (unset = {})[path] = 1
      query.val = { $unset: unset }
    else if qmethod == 'update'
      if (unset = query.val.$unset) && objEquiv qconds, conds
        unset[path] = 1
      else
        # Either the existing query involves another $atomic, or the
        # conditions of the existing query do not match the incoming
        # op conditions. In both cases, we must create a new query
        nextQuery = {}
        [nextQuery] = @set nextQuery, conds, path, val, ver
    else
      # The current query involves
      nextQuery = {}
      [nextQuery] = @set nextQuery, conds, path, val, ver
    return [query, nextQuery]

  push: (query, conds, path, values...) ->
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

      (args = {})[path] = val
      (query.val ||= {})[k] = args
    else if qmethod == 'update'
      if objEquiv qconds, conds
        if query.val.$push
          if existingPush = query.val.$push[path]
            query.val.$pushAll = {}
            query.val.$pushAll[path] = [existingPush, values...]
            delete query.val.$push
          else
            nextQuery = {}
            [nextQuery] = @push nextQuery, conds, path, values...
        else if query.val.$pushAll
          nextQuery = {}
          [nextQuery] = @push nextQuery, conds, path, values...
        else
          # Then the prior ops involved something like $set
          nextQuery = {}
          [nextQuery] = @push nextQuery, conds, path, values...
      else
        # Current building query involves conditions not equal to
        # current op conditions, so create a new query
        nextQuery = {}
        [nextQuery] = @push nextQuery, conds, path, values...
    else
      nextQuery = {}
      [nextQuery] = @push nextQuery, conds, path, val, ver
    return [query, nextQuery]

  pop: (query, path, values..., ver) ->

operation =
  splat: ([conds, method, args...]) -> {conds, method, args}
