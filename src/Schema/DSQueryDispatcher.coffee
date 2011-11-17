FindBuilder = require './Data/FindBuilder'
FindOneBuilder = require './Data/FindOneBuilder'
Promise = require '../Promise'
{deepEqual} = require '../util'

DSQueryDispatcher = module.exports = (@_queryMethod) ->
  @_buildersByPhase = []
  @_logicalFieldsPromises = []
  return

DSQueryDispatcher:: =
  add: (logicalField, conds) ->
    unless logicalField.dataFields.length
      return console.warn """`#{logicalField.path}` is a declared logical field of
        schema `#{logicalField.schema._name}, but does not correspond to any
        data schema component. Please add it to one of your data schemas."""

    dataFieldReadPhases = logicalField.dataFieldReadPhases || logicalField.genDataFieldReadPhases()
    lastPhase           = dataFieldReadPhases.length - 1
    switch queryMethod = @_queryMethod
      when 'find'    then onFound = @_onFind   ; noneFoundPredicate = @_didntFind
      when 'findOne' then onFound = @_onFindOne; noneFoundPredicate = @_didntFindOne

    lFieldPromise = new Promise # Promises the logical field value
    @_logicalFieldsPromises.push lFieldPromise
    dFieldPromCb = (err, val, dataField) ->
      onFound err, val, dataField, lFieldPromise

    # For each phase, place the data fields assoc with that phase
    # into a minimum number of queries. Wire those queries together
    # so that certain queries trigger other queries.
    for [dataFields, parallelCallback], phase in dataFieldReadPhases
      dFieldPromises = []
      for dField in dataFields
        dFieldPromises.push dFieldProm = new Promise(bothback: dFieldPromCb)
        # TODO Transform conds based on dField and dField.schema
        if dField.query
          # Handles many or one inverse of Ref
          _conds = dField.query._conditions # typeof _conds[k] == 'function'
          _conds[k] = _conds[k](conds) for k, v of _conds
          conds = _conds
          queryMethod = dField.query.queryMethod
        qb = @_findOrCreateQueryBuilder phase, dField.source, dField.ns, conds, queryMethod
        qb.add dField, dFieldProm
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
    buildersByHash = @_buildersByPhase[0]
    for _, builders of buildersByHash
      for builder in builders
        query = builder.toQuery()
        query.fire builder.queryCallback
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
  _findOrCreateQueryBuilder: (phase, source, ns, conds, queryMethod) ->
    buildersByHash = @_buildersByPhase[phase] ||= {}
    hash = @_hash source, ns
    builders = buildersByHash[hash] ||= []
    for qb in builders
      return qb if deepEqual qb.conds, conds
    console.log "CONDS"
    console.log conds
    qb = switch queryMethod
      when 'find'    then new FindBuilder conds
      when 'findOne' then new FindOneBuilder conds
    builders.push qb
    return qb
  
  _hash: (source, ns) -> source._name + '.' + ns

  _notifyNextPhase: (phase, logicalField, didntFind) ->
    buildersByHash = @_buildersByPhase[phase + 1]
    for _, builders of buildersByHash
      for qb in builders
        qb.notifyAboutPrevQuery logicalField, didntFind
    return

  _didntFind: ([metas, dataField]) ->
    return true if metas is undefined || !metas.length
    for {val} in metas
      return true unless val?
    return false
  _didntFindOne: ([val, dataField]) -> ! val?
