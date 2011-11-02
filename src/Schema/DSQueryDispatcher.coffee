DSQuery = require './DSQuery'
Promise = require '../Promise'
{deepEqual} = require '../util'

DSQueryDispatcher = module.exports = (@_queryMethod) ->
  @_queriesByPhase = []
  @_logicalFieldsPromises = []
  return

DSQueryDispatcher:: =
  _didNotFind: ([metas, dataField]) ->
    return true if metas is undefined
    return true unless metas.length
    for {val} in metas
      return true unless val?
    return false
  _didNotFindOne: ([val, dataField]) -> ! val?

  registerLogicalField: (logicalField, conds) ->
    unless logicalField.dataFields.length
      console.warn "`#{logicalField.path}` is a declared logical field of schema `#{logicalField.schema._name}, but does not correspond to any data schema component. Please add it to one of your data schemas."
      return

    self = this
    queryMethod = @_queryMethod
    @_logicalFieldsPromises.push lFieldPromise = new Promise
    dataFieldFlow = logicalField.dataFieldFlow || logicalField.genDataFieldFlow()
    lastPhase = dataFieldFlow.length - 1
    fieldHandler = @['_' + queryMethod + 'FieldHandler']
    # Promise for the future fetched data field value
    dFieldPromCb = (err, val, dataField) -> fieldHandler err, val, dataField, lFieldPromise
    noneFoundPredicate = if queryMethod == 'find' then @_didNotFind else @_didNotFindOne
    for [dataFields, parallelCallback], phase in dataFieldFlow
#      if dataFields[0].dependsOn
#        # TODO
#        continue
      dFieldPromises = []
      for dField in dataFields
        dFieldPromises.push dFieldProm = new Promise
        # TODO Transform conds based on dField and dField.schema
        q = @_findOrCreateQuery phase, dField.source, dField.ns, conds, queryMethod
        q.add dField, dFieldProm
        # TODO Why have 1 dFieldProm per dField from a dataFields that all belong to the same data source?
        dFieldProm.bothback dFieldPromCb

      phasePromise = Promise.parallel dFieldPromises...
      do (parallelCallback, phase) ->
        phasePromise.bothback (err, vals...) ->
          # TODO Handle err
          parallelCallback vals... if parallelCallback
          noneFound = vals.every noneFoundPredicate
          if phase == lastPhase
            lFieldPromise.fulfill undefined if noneFound
          else
            self._notifyPhase phase + 1, logicalField, noneFound

  _findOneFieldHandler: (err, val, dataField, logicalFieldPromise) ->
    # TODO handle err
    # TODO Alternatively to logicalFieldPromise.fulfilled lazy check, we can eagerly remove dataFieldProm's callbacks upon an logicalFieldPromise fulfillment
    return if val is undefined || logicalFieldPromise.fulfilled
    val = dataField.type.uncast val if dataField.type?.uncast
    return logicalFieldPromise.fulfill val

  _findFieldHandler: (err, values, dataField, logicalFieldPromise) ->
    return if values is undefined || !values.length || logicalFieldPromise.fulfilled
    # TODO modify this to handle pagination
    dataType = dataField.type
    for meta in values
      if dataType.uncast
        meta.val = dataType.uncast meta.val
    return logicalFieldPromise.fulfill values

  # @param {Function} callback(err, val)
  fire: (callback) ->
    queriesByHash = @_queriesByPhase[0]
    for _, queries of queriesByHash
      q.fire() for q in queries
    allPromise = Promise.parallel @_logicalFieldsPromises...
    allPromise.bothback callback if callback
    return allPromise

  # @param {Number} phase
  # @param {DataSource} source
  # @param {String} ns
  # @param {Object} conds
  _findOrCreateQuery: (phase, source, ns, conds) ->
    queriesByHash = @_queriesByPhase[phase] ||= {}
    hash = @_hash source, ns
    queries = queriesByHash[hash] ||= []
    for q in queries
      return q if deepEqual q.conds, conds
    q = new DSQuery conds, @_queryMethod
    queries.push q
    return q
  
  _hash: (source, ns) -> source._name + '.' + ns

  _notifyPhase: (phase, logicalField, didNotFind) ->
    queriesByHash = @_queriesByPhase[phase]
    for _, queries of queriesByHash
      q.notifyAboutPrevQuery logicalField, didNotFind for q in queries
    return
