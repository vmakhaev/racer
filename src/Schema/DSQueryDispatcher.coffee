DSQuery = require './DSQuery'
Promise = require '../Promise'
{deepEqual} = require '../util'

DSQueryDispatcher = module.exports = (@_queryMethod) ->
  @_queriesByPhase = []
  @_logicalFieldsPromises = []
  return

DSQueryDispatcher:: =
  add: (logicalField, conds) ->
    unless logicalField.dataFields.length
      return console.warn """`#{logicalField.path}` is a declared logical field of
        schema `#{logicalField.schema._name}, but does not correspond to any
        data schema component. Please add it to one of your data schemas."""

    dataFieldFlow = logicalField.dataFieldFlow || logicalField.genDataFieldFlow()
    lastPhase     = dataFieldFlow.length - 1
    switch queryMethod = @_queryMethod
      when 'find'    then onFound = @_onFind   ; noneFoundPredicate = @_didntFind
      when 'findOne' then onFound = @_onFindOne; noneFoundPredicate = @_didntFindOne

    lFieldPromise = new Promise # Promises the logical field value
    @_logicalFieldsPromises.push lFieldPromise
    dFieldPromCb = (err, val, dataField) ->
      onFound err, val, dataField, lFieldPromise

    # For each phase, place the data fields assoc with that phase
    # into a minimum number of queries. Fire those queries.
    for [dataFields, parallelCallback], phase in dataFieldFlow
      dFieldPromises = []
      for dField in dataFields
        dFieldPromises.push dFieldProm = new Promise(bothback: dFieldPromCb)
        # TODO Transform conds based on dField and dField.schema
        if dField.query
          # Handles many or one inverse of Ref
          _conds = dField.query._conditions # typeof _conds[k] == 'function'
          _conds[k] = _conds[k](conds) for k, v of _conds
          conds = _conds
          fieldQueryMethod = dField.query.queryMethod
        q = @_findOrCreateQuery phase, dField.source, dField.ns, conds, fieldQueryMethod || queryMethod
        q.add dField, dFieldProm
        # TODO Why have 1 dFieldProm per dField from a dataFields that all belong to the same data source?

      self = this
      phasePromise = Promise.parallel dFieldPromises...
      do (parallelCallback, phase) ->
        phasePromise.bothback (err, vals...) ->
          # vals = [ [fetchedVal, dataField], ... ]
          console.log vals
          # TODO Handle err
          parallelCallback vals... if parallelCallback
          noneFound = vals.every noneFoundPredicate
          if phase == lastPhase
            lFieldPromise.fulfill undefined if noneFound
          else
            self._notifyNextPhase phase, logicalField, noneFound

  # @param {Function} callback(err, val)
  fire: (callback) ->
    queriesByHash = @_queriesByPhase[0]
    for _, queries of queriesByHash
      q.fire() for q in queries
    allPromise = Promise.parallel @_logicalFieldsPromises...
    allPromise.bothback callback if callback
    return allPromise

  _onFindOne: (err, val, dataField, logicalFieldPromise) ->
    return logicalPromise.error err if err
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

  # @param {Number} phase
  # @param {DataSource} source
  # @param {String} ns
  # @param {Object} conds
  _findOrCreateQuery: (phase, source, ns, conds, queryMethod) ->
    queriesByHash = @_queriesByPhase[phase] ||= {}
    hash = @_hash source, ns
    queries = queriesByHash[hash] ||= []
    for q in queries
      return q if deepEqual q.conds, conds
    console.log "CONDS"
    console.log conds
    q = new DSQuery conds, queryMethod
    queries.push q
    return q
  
  _hash: (source, ns) -> source._name + '.' + ns

  _notifyNextPhase: (phase, logicalField, didntFind) ->
    queriesByHash = @_queriesByPhase[phase + 1]
    for _, queries of queriesByHash
      for q in queries
        q.notifyAboutPrevQuery logicalField, didntFind
    return

  _didntFind: ([metas, dataField]) ->
    return true if metas is undefined || !metas.length
    for {val} in metas
      return true unless val?
    return false
  _didntFindOne: ([val, dataField]) -> ! val?
