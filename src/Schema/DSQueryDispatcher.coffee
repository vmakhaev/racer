DSQuery = require './DSQuery'
{deepEqual} = require '../util'

DSQueryDispatcher = module.exports = (@_queryMethod) ->
  @_queriesByPhase = []
  @_colHashes = []
  @_logicalFieldsPromises = []
  return

DSQueryDispatcher:: =
  registerLogicalField: (logicalField) ->
    @_logicalFieldsPromises.push lFieldPromise = new Promise
    dataFieldFlow = logicalField.dataFieldFlow || logicalField.genDataFieldFlow()
    lastIfNoneFoundPromise = null
    for [dataFields, parallelCallback], phase in dataFieldFlow
#      if dataFields[0].dependsOn
#        # TODO
#        continue
      dFieldPromises = []
      for dField in dataFields
        q = @_findOrCreateQuery phase, dField.source, dField.ns, conds, @_queryMethod
        q.add dField

        dFieldPromises.push dFieldProm = new Promise
        do (lFieldPromise, dField) ->
          dFieldProm.bothback (err, val) ->
            # TODO handle err
            return if val is undefined
            # TODO modify this to handle pagination
            return lFieldPromise.fulfill val unless dField.deref
            derefProm = dField.deref val
            return derefProm.bothback (err, val) -> lFieldProm.resolve val
      # Handle when we don't find the value in any of the current parallel data fields
      ifNoneFoundPromise = Promise.parallel dFieldPromises...
      do (parallelCallback) ->
        ifNoneFoundPromise.bothback (err, vals...) ->
          # TODO Handle err
          parallelCallback vals...
      if lastIfNoneFoundPromise
        self = this
        do (dataFieldFlow, phase) ->
          lastIfNoneFoundPromise.bothback (err, vals...) ->
            # TODO Handle err
            noneFound = ! vals.some ([val]) -> val?
            self._notifyPhase phase, logicalField, noneFound
      lastIfNoneFoundPromise = ifNoneFoundPromise

  fire: (callback) ->
    queries = @_queriesByPhase[0]
    q.fire() for q in queries
    allPromise = Promise.parallel @_logicalFieldsPromises...
    allPromise.bothback callback if callback
    return allPromise

  _findOrCreateQuery: (phase, source, ns, conds) ->
    queries = @_queriesByPhase[phase] ||= []
    colHash = @_hash source, ns
    col = @_colHashes.indexOf colHash
    if col == -1
      col = colHashes.push colHash
    queries = queries[col] ||= []
    for q in queries
      return q if deepEqual q.conds, conds
    q = new Query conds, @_queryMethod
    queries.push q
    return q
  
  _hash: (source, ns) -> source + '.' + ns

  _notifyPhase: (phase, logicalField, didNotFind) ->
    queries = @_queriesByPhase[phase]
    q.notifyAboutPrevQuery logicalField, didNotFind for q in queries
    return
