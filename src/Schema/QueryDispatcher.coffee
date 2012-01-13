FindBuilder = require './Data/FindBuilder'
FindOneBuilder = require './Data/FindOneBuilder'
Promise = require '../Promise'
{deepEqual} = require '../util'

# There's one of these per LogicalQuery. QueryDispatchers are responsible for
# turning a set of (logical field, conditions) pairs into a set of queries on 1
# or or more databases.
#
# These queries are bound to a (deduced) flow control that determines a serial
# order of parallel queries that will be fired off. This flow is deduced
# based on data dependencies (e.g., we only know to fetch certain
# documents once we have fetched a document that contains a foreign key
# pointing to those documents). No data dependencies means we can fire all
# queries off at once in parallel.
#
# There can be data dependencies:
# - Between 2 data fields we want to select in the same db
# - Between 2 data fields we want to select from different dbs (possibly linked
#   to the same logical field)
# - Between a condition value and the document that owns it
QueryDispatcher = module.exports = (@_queryMethod) ->
  @_buildersByPhase = []

  # TODO Convert [] -> {} to exploit Promise.parallel({...}) form
  #      This would reduce accidental complexity in LogicalQuery::fire's qDispatcher.fire callback
  @_logicalFieldsPromises = []
  return

QueryDispatcher:: =
  # Given logicalField and conds,
  # - Either create a new database query AND position the query in the flow
  #   control of database queries
  # - Or add it to an existing query AND maybe re-position the query in the
  #   flow control of database queries
  add: (logicalField, conds) ->
    unless logicalField.dataFields.length
      return console.warn "`#{logicalField.path}` is a declared logical field of schema `#{logicalField.schema._name}, but does not correspond to any data schema component. Please add it to one of your data schemas."

    # This has the form [ dataFieldA, dataFieldB, ... ] where the numeric
    # phases correspond to the numeric array indices
    dataFieldReadPhases = logicalField.dataFieldReadPhases || logicalField.genDataFieldReadPhases()
    lastPhase           = dataFieldReadPhases.length - 1

    # TODO Exploit polymorphism instead of keeping a @_queryMethod
    switch queryMethod  = @_queryMethod
      when 'find'    then onFound = @_onFind   ; noneFoundPredicate = @_didntFind
      when 'findOne' then onFound = @_onFindOne; noneFoundPredicate = @_didntFindOne

    lFieldPromise = new Promise # Promises the logical field value
    @_logicalFieldsPromises.push lFieldPromise
    dFieldPromCb = (err, val, dataField) ->
      onFound err, val, dataField, lFieldPromise

    # For each phase, place the data fields assoc with that phase
    # into a minimum number of queries. Wire those queries together
    # so that certain queries trigger other queries.
    for dataFields, phase in dataFieldReadPhases
      {parallelCallback} = dataFields
      dFieldPromises = []
      for dField in dataFields
        dFieldProm = new Promise(bothback: dFieldPromCb)
        dFieldPromises.push dFieldProm

        qb = @_findOrCreateQueryBuilder phase, dField, conds, queryMethod
        unless dField.isVirtual
          qb.add dField, dFieldProm
        else if qb instanceof FindBuilder
          do (dField, dFieldProm) ->
            if -1 != Object.keys(conds).indexOf dField.pkey
              query = dField.querify conds
            else
              # TODO Trigger the query once we fetch the value of dField.pkey
              console.log "%%%%%%%%%%%%%%"
            qb.toQuery = -> query
            qb.queryCallback = (err, arr) ->
              inversePath = dField.fkey
#                for mem in arr
#                  # TODO This delete is a hack; instead, resolve the foreign key to the doc it points to
#                  #      , i.e., to the existing document that was used to generate this query
#                  delete mem[inversePath]
              dFieldProm.resolve err, arr, dField
        else if qb instanceof FindOneBuilder
          do (dField, dFieldProm) ->
            query = dField.querify conds
            qb.toQuery = -> query
            qb.queryCallback = (err, json) ->
              inversePath = dField.fkey
#                # TODO This delete is a hack; instead, resolve the foreign key to the document it points to
#                #      , i.e., to the existing document that was used to generate this query
#                delete json[inversePath]
              dFieldProm.resolve err, json, dField
        else
          throw new Error 'Unimplemented'

#        switch dField.type._name
#          when 'Virtual'
#            query = dField.querify conds
#            qb = @_addQueryAsQueryBuilder phase, query
#            qb.promises ||= []
#            qb.promises.push [dFieldProm, dField]
#            qb.queryCallback = (err, arr) ->
#              for [_dFieldProm, _dField] in @promises
##                  delete json[_dField.fkey]
#                _dFieldProm.resolve err, arr, _dField
#          else
#            # TODO Transform conds based on dField and dField.schema
#            {source, ns} = dField
#            qb = @_findOrCreateQueryBuilder(phase, dField, conds, dField.queryMethod || queryMethod)
#            qb.add dField, dFieldProm
#            # TODO Why have 1 dFieldProm per dField from a dataFields that all belong to the same data source?

      self = this
      phasePromise = Promise.parallel dFieldPromises
      do (parallelCallback, phase) ->
        phasePromise.bothback (err, vals...) ->
          throw err if err # TODO Handle err better
          # vals = [ [fetchedVal, dataField], ... ]
          parallelCallback vals... if parallelCallback
          noneFound = vals.every noneFoundPredicate
          if phase == lastPhase
            lFieldPromise.fulfill undefined if noneFound
          else
            self._notifyNextPhase phase, logicalField, noneFound

  # @param {Function} callback(err, val)
  fire: (callback) ->
    buildersByHash = @_buildersByPhase[0]
    for _, builders of buildersByHash
      for builder in builders
        query = builder.toQuery()
        do (builder) ->
          query.fire (err, json) ->
            builder.queryCallback err, json
    allPromise = Promise.parallel @_logicalFieldsPromises
    allPromise.bothback callback if callback
    return allPromise

  _onFindOne: (err, val, dataField, logicalFieldPromise) ->
    return logicalFieldPromise.error err if err
    # TODO Alternatively to logicalFieldPromise.fulfilled lazy check, we can eagerly remove dataFieldProm's callbacks upon an logicalFieldPromise fulfillment
    return if val is undefined || logicalFieldPromise.fulfilled
    val = dataField.type.uncast val if dataField.type?.uncast
    return logicalFieldPromise.fulfill val

  _onFind: (err, values, dataField, logicalFieldPromise) ->
    return if values is undefined || !values.length || logicalFieldPromise.fulfilled
    # TODO modify this to handle pagination
    dataType = dataField.type
    for meta in values
      if dataType.uncast
        meta.val = dataType.uncast meta.val
    return logicalFieldPromise.fulfill values

  # TODO Eliminate branching in code to either _findOrCreateQueryBuilder or _addQueryAsQueryBuilder. Better way to do this polymorphically?


  _findOrCreateQueryBuilder: (phase, dField, conds, queryMethod) ->
#    if dField.isVirtual && Object.keys(conds).indexOf(dField.pkey) == -1
#      phase++
    buildersByHash = @_buildersByPhase[phase] ||= {}
    if query = dField.querify?(conds)
      # Is the case for Virtual DataFields
      {source, ns} = DataSkema = query.schema
      conds = query._conditions
      queryMethod = query.queryMethod
    else
      {source, ns} = dField
      DataSkema = source.dataSchemasWithNs[ns]
    hash = @_hash source, ns
    builders = buildersByHash[hash] ||= []
    for qb in builders
      # TODO deepEqual could be expensive
      return qb if deepEqual qb.conds, conds
    qb = switch queryMethod
      when 'find'    then new FindBuilder DataSkema, conds
      when 'findOne' then new FindOneBuilder DataSkema, conds
    builders.push qb
    return qb

  # TODO REMOVE WITH A
#  # @param {Number} phase
#  # @param {DataSource} source
#  # @param {String} ns
#  # @param {Object} conds
#  # @param {String} queryMethod
#  _findOrCreateQueryBuilder: (phase, source, ns, conds, queryMethod) ->
#    buildersByHash = @_buildersByPhase[phase] ||= {}
#    hash = @_hash source, ns
#    builders = buildersByHash[hash] ||= []
#    for qb in builders
#      return qb if deepEqual qb.conds, conds
#    qb = switch queryMethod
#      when 'find'    then new FindBuilder source, conds
#      when 'findOne' then new FindOneBuilder source, conds
#    builders.push qb
#    return qb
#
#  _addQueryAsQueryBuilder: (phase, query) ->
#    buildersByHash = @_buildersByPhase[phase] ||= {}
#    {source, ns} = query.schema
#    hash = @_hash source, ns
#    builders = buildersByHash[hash] ||= []
#    qb =
#      conds: query._conditions
#      toQuery: -> query
#      # TODO Define qb.notifyAboutPrevQuery
#    builders.push qb
#    return qb

  _hash: (source, ns) -> source._name + '.' + ns

  _notifyNextPhase: (phase, logicalField, didFind) ->
    buildersByHash = @_buildersByPhase[phase + 1]
    for _, builders of buildersByHash
      for qb in builders
        qb.notifyAboutPrevQuery logicalField, didFind
    return

  _didntFind: ([metas, dataField]) ->
    return true if metas is undefined || !metas.length
    for {val} in metas
      return true unless val?
    return false
  _didntFindOne: ([val, dataField]) -> ! val?
