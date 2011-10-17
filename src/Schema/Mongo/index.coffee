{objEquiv} = require '../../util'
DataSource = require '../DataSource'
types = require './types'

# Important roles are:
# - Convert oplog to db queries
# - Define special types specific to the data store (e.g., ObjectId)
MongoSource = module.exports = DataSource.extend
  AdapterClass: require './adapter'
  inferType: (descriptor) ->
    if Array.isArray descriptor
      arrayType = types['Array']
      memberType = descriptor[0]
      concreteArrayType = Object.create arrayType
      concreteArrayType.memberType = @inferType memberType
    if type = types[descriptor.name]
      return type
    # else String, Number etc
    return {}

  _queriesForOps: (oplog) ->
    queries = []
    query = {}
    for op in oplog
      {ns, conds, method, path, args} = operation.splat op
      # TODO Handle nested paths
      continue unless field = @fields[ns][path]
      [query, nextQuery] = @[method] ns, field, query, conds, args...
      if nextQuery
        queries.push @_compileQuery query
        query = nextQuery

    if query
      queries.push @_compileQuery query

    return queries

  # Better to build a query out of multiple ops using
  # a pre-compiled form; then post-compile the query for
  # use by the adapter once the query is done being built.
  # Pre-compiled will look like:
  #   query.ns
  #   query.method
  #   query.conds
  #   query.val
  #   query.opts
  # Post-compiled will look like:
  #   query.method
  #   query.args = [query.ns, query.conds, query.val, query.opts]
  _compileQuery: (query) ->
    args = query.args = [query.ns]
    opts = query.opts ||= {}
    opts.safe = true
    if query.method == 'update'
      opts.upsert = true
      args.push query.conds
    args.push query.val, opts
    return query

  # e.g., accomplish optimizations such as collapsing
  # multiple sequental push ops into a single atomic push
  _minifyOps: (oplog) -> oplog

  # TODO Better lang via MongoQueryBuilder.handle 'set', (...) -> ?
  set: (ns, field, query, conds, path, val, ver) ->
    val = field.cast val if field.cast

    # Assign or augment query.(method|conds|val)
    {ns: qns, method: qmethod, conds: qconds} = query
    if qmethod is undefined && qconds is undefined
      query.ns = ns
      if query.conds = conds
        query.method = 'update'
        (delta = {})[path] = val
        query.val = { $set: delta }
      else
        query.method = 'insert'
        query.val = query.val = {}
        query.val[path] = val
    else if qmethod == 'update'
      if (delta = query.val.$set) && objEquiv qconds, conds
        delta[path] = val
      else
        nextQuery = {}
        [nextQuery] = @set ns, field, nextQuery, conds, path, val, ver
    else if qmethod == 'insert'
      if ns != qns || qconds isnt undefined
        nextQuery = {}
        [nextQuery] = @set ns, field, nextQuery, conds, path, val, ver
      else
        query.val[path] = val
    else
      nextQuery = {}
      [nextQuery] = @set ns, field, nextQuery, conds, path, val, ver
    return [query, nextQuery]

  del: (ns, field, query, conds, path) ->
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
        [nextQuery] = @del ns, field, nextQuery, conds, path, val, ver
    else
      # The current query involves
      nextQuery = {}
      [nextQuery] = @del ns, field, nextQuery, conds, path, val, ver
    return [query, nextQuery]

  push: (ns, field, query, conds, path, values...) ->
    # Assign or augment query.(method|conds|val)
    {ns: qns, method: qmethod, conds: qconds} = query
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
            [nextQuery] = @push ns, field, nextQuery, conds, path, values...
        else if query.val.$pushAll
          nextQuery = {}
          [nextQuery] = @push ns, field, nextQuery, conds, path, values...
        else
          # Then the prior ops involved something like $set
          nextQuery = {}
          [nextQuery] = @push ns, field, nextQuery, conds, path, values...
      else
        # Current building query involves conditions not equal to
        # current op conditions, so create a new query
        nextQuery = {}
        [nextQuery] = @push ns, field, nextQuery, conds, path, values...
    else if qmethod == 'insert'
      if ns != qns
        nextQuery = {}
        [nextQuery] = @push ns, field, nextQuery, conds, path, values...
      else
        arr = query.val[path] ||= []
        arr.push values...
    else
      nextQuery = {}
      [nextQuery] = @push ns, field, nextQuery, conds, path, val, ver
    return [query, nextQuery]

  pop: (ns, field, query, path, values..., ver) ->
    throw new Error 'Unimplemented'

operation =
  splat: ([namespace, conds, method, args...]) -> {ns: namespace, conds, method, path: args[0], args}
