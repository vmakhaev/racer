DSQuery = require './DSQuery'
Promise = require '../Promise'
{deepEqual} = require '../util'

DSQueryDispatcher = module.exports = (@_queryMethod) ->
  @_queriesByPhase = []
  @_logicalFieldsPromises = []
  return

DSQueryDispatcher:: =
  registerLogicalField: (logicalField, conds) ->
    unless logicalField.dataFields.length
      console.warn "`#{logicalField.path}` is a declared logical field of schema `#{logicalField.schema._name}, but does not correspond to any data schema component. Please add it to one of your data schemas."
      return
    @_logicalFieldsPromises.push lFieldPromise = new Promise
    dataFieldFlow = logicalField.dataFieldFlow || logicalField.genDataFieldFlow()
    lastIfNoneFoundPromise = null
    for [dataFields, parallelCallback], phase in dataFieldFlow
#      if dataFields[0].dependsOn
#        # TODO
#        continue
      dFieldPromises = []
      for dField in dataFields
        dFieldPromises.push dFieldProm = new Promise
        # TODO Transform conds based on dField and dField.schema
        q = @_findOrCreateQuery phase, dField.source, dField.ns, conds, @_queryMethod
        q.add dField, dFieldProm
        do (dField) ->
          dFieldProm.bothback (err, val) ->
            # TODO handle err
            return if val is undefined
            # TODO modify this to handle pagination
            unless dField.deref || lFieldPromise.fulfilled
              val = dField.type.uncast val if dField.type?.uncast
              return lFieldPromise.fulfill val
            derefProm = dField.deref val
            return derefProm.bothback (err, val) ->
              lFieldPromise.resolve err, val
      # Handle when we don't find the value in any of the current parallel data fields

      # A promise for if none of the parallel data sources for this phase
      # return data
      ifNoneFoundPromise = Promise.parallel dFieldPromises...
      do (parallelCallback) ->
        ifNoneFoundPromise.bothback (err, vals...) ->
          # TODO Handle err
          parallelCallback vals... if parallelCallback
          noneFound = ! vals.some ([val]) -> val?
          lFieldPromise.fulfill undefined if noneFound
      if lastIfNoneFoundPromise
        self = this
        do (dataFieldFlow, phase) ->
          lastIfNoneFoundPromise.bothback (err, vals...) ->
            # TODO Handle err
            noneFound = ! vals.some ([val]) -> val?
            self._notifyPhase phase, logicalField, noneFound
      lastIfNoneFoundPromise = ifNoneFoundPromise

  fire: (callback) ->
    queriesByHash = @_queriesByPhase[0]
    for _, queries of queriesByHash
      q.fire() for q in queries
    allPromise = Promise.parallel @_logicalFieldsPromises...
    allPromise.bothback callback if callback
    return allPromise

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
