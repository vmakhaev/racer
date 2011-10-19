{objEquiv} = require '../../util'
DataSource = require '../DataSource'
types = require './types'

# Important roles are:
# - Convert oplog to db queries
# - Define special types specific to the data store (e.g., ObjectId)
MongoSource = module.exports = DataSource.extend

  # The layer of abstraction that deals with db-specific commands
  AdapterClass: require './adapter'

  types: types

  # Where the magical interpretation happens of the right-hand-side vals
  # of attributes in config of
  #     CustomSchema.source source, ns, config
  inferType: (descriptor) ->
    if descriptor.constructor == Object
      if '$type' of descriptor
        # If we have { $type: SomeType, ... } as our Data Source Schema attribute rvalue
        type = @inferType descriptor.$type
        delete descriptor.$type
        for flag, arg of descriptor
          if 'function' == typeof type[flag]
            if Array.isArray arg
              type[flag] arg...
            else
              type[flag] arg
          else if type[flag] is undefined
            type[flag] = arg
          else
            throw new Error "Unsupported type descriptor flag #{flag}"
        return type
      if '$dataField' of descriptor
        {source, schema, fieldName, type: ftype} = descriptor.$ref
        if ftype.$pkey # If this field is a ref
          # source.inferType 
          return source.types.Ref.createField
            pkeyType: ftype.$type
            pkeyName: ftype.fieldName
        # if array ref
        # if inverse ref
        # if inverse array ref

    if Array.isArray descriptor
      arrayType = types['Array']
      memberType = descriptor[0]
      concreteArrayType = Object.create arrayType
      concreteArrayType.memberType = @inferType memberType
      return concreteArrayType

    if type = types[descriptor.name || descriptor._name]
      return type

    # else String, Number, Object => Take things as they are
    return {}

  # @param {Array} oplog
  # @param {Function} applyOpsCallback(err, extraAttrs)
  _queriesForOps: (oplog, applyOpsCallback) ->
    queries = []
    query = {}
    for op in oplog
      {ns, conds, method, path, args} = operation.splat op
      # TODO Handle nested paths
      continue unless field = @fields[ns][path]
      [query, nextQuery, foreignQueries] = @[method] ns, field, query, conds, args...
      if foreignQueries
        # a foreignQuery would result because
        # of an operation involving a reference, 
        # array reference, inverse reference, 
        # or inverse array reference
        # TODO Be smarter about the relative ordering of queries
        queries = foreignQueries.concat queries
      else if nextQuery
        queries.push @_compileQuery query
        query = nextQuery

    if query
      queries.push @_compileQuery query

    return queries

  _oplogToQuerySet: (oplog, querySet = new QuerySet) ->
    for op in oplog
      {ns, conds, method, path, args} = operation.splat op

      # TODO Handle nested paths

      # Filter out paths that don't map to this Data Source
      continue unless field = @fields[ns][path]
      @[method] querySet, ns, field, conds, args...
    return querySet

  # e.g., accomplish optimizations such as collapsing
  # multiple sequental push ops into a single atomic push
  _minifyOps: (oplog) -> oplog

  # TODO Better lang via MongoQueryBuilder.handle 'set', (...) -> ?
  # @param {String} ns is the namespace
  # @param {Object} field is the data source field
  # @param {Object} query is the current query we've been building
  # @param {Object} conds is the oplog op's conds
  # @param {String} path is the oplog op's path
  # @param {Object} val is the oplog op's val
  # @param {Number} ver is the oplog op's ver
  # TODO Deprecate this version of set
  set: (ns, field, query, conds, path, val, ver) ->
    if field._name == 'Ref'
      # val is an object literal, that needs to be stored
      unless val._id # If new
        ForeignSkema = field.Skema
        targetDoc = new ForeignSkema val
        targetQueries = source._queriesForOps targetDoc.oplog
        # TODO Hmmm, unfortunately, the foreign key that we need to add to query depends on running targetQueries first.
        field = field.pkeyType
        val = '????'
        @set ns, field, query, conds, path, val, ver

#        ForeignSkema.applyOps targetDoc.oplog, (err, doc) ->
#          # TODO Replace with callback err
#          throw err if err
#          @set ns, field.pkeyType, query, conds, path, doc.get('_id'), ver

        return [query, null, targetQueries]

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
        query.val = {}
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

  set: (querySet, ns, field, conds, path, val, ver) ->
    query = querySet.findOrCreateQuery ns, conds, 'set'
    if field._name == 'Ref'
      pkeyName = field.pkeyName
      unless val[pkeyName] # If is new
        # val is an object literal that needs to be stored as a Mongo doc
        # and then whose pkey needs to be assigned to the current path
        # on the doc defined by the conds lookup in ns
        {Skema: ForeignSkema, source} = field
        targetDoc = new ForeignSkema val
        targetQuerySet = source._oplogToQuerySet targetDoc.oplog
        targetQuery = targetQuerySet.singleQuery
        targetQuery.add targetQuery
        querySet.pipe targetQuery.extraAttr(pkeyName), query.setAs(path)
        return querySet

    val = field.cast val if field.cast

    # Add or augment query with query.(method|conds|val)
    {ns: qns, method: qmethod, conds: qconds} = query

    switch qmethod
      when undefined
        if qconds
          query.method = 'update'
          (delta = {})[path] = val
          query.val = { $set: delta }
          return querySet
        query.method = 'insert'
        query.val = {}
        query.val[path] = val
      when 'update'
        query.val.$set[path] = val
      when 'insert'
        query.val[path] = val
      else
        throw new Error 'Implement for other incomnig qmethods - e.g., push, pushAll, etc'
    return querySet

  del: (ns, field, query, conds, path) ->
    # Assign or augment query.(method|conds|val)
    {method: qmethod, conds: qconds} = query
    if qmethod is undefined && qconds is undefined
      query.ns = ns
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
    values = field.cast values if field.cast
    # Assign or augment query.(method|conds|val)
    {ns: qns, method: qmethod, conds: qconds} = query
    if qmethod is undefined && qconds is undefined
      query.ns = ns
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
