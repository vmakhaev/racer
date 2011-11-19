{merge} = require '../../util'
AbstractQuery = require '../AbstractQuery'
Promise = require '../../Promise'

DataQuery = module.exports = (schema, criteria) ->
  AbstractQuery.call @, schema, criteria
  return

DataQuery:: = merge new AbstractQuery(),
  # Options methods

  # TODO Do something with @_fields
  # @param {String->Field} fields
  fields: (@_fields) ->

  fire: (fireCallback) ->
    conds = @_castConditions()

    # 2. Determine if we should generate any other queries
    #    e.g., for queries that include a Ref
    #    QueryDispatcher?

    # TODO Handle selects
#    selects = @_selects
#    if selects.length
#      dataFields = (RootDataSkema.lookupField path for path in selects)
#    else
#      # TODO Filter this?
#      dataFields = ([field, ''] for _, field of RootDataSkema.fields)

    firePromise = new Promise bothback: fireCallback

    switch @queryMethod
      when 'find'    then @_findFire    firePromise
      when 'findOne' then @_findOneFire firePromise

    return firePromise

  _findOneFire: (firePromise) ->
    {source, ns, fields} = @schema
    source.adapter.findOne ns, @_conditions, {}, (err, json) ->
      return firePromise.fail    err        if err
      return firePromise.resolve null, null unless json
      derefPromises = []
      # TODO Part of the following block is duplicated in DataSource::castObj
      for path, val of json
        resField = fields[path]
        if resField.deref then do (path) ->
          derefProm = resField.deref val, (err, dereffedJson) ->
            # Uncast using the referenced data source schema
#              json[path] = resField.type.uncast dereffedJson if resField.type.uncast
            json[path] = dereffedJson
          derefPromises.push derefProm
        else
          json[path] = resField.cast val if resField.cast
      switch derefPromises.length
        when 0
          return firePromise.resolve null, json
        when 1
          adapterProm = derefPromises[0]
        else
          adapterProm = Promise.parallel derefPromises
      return adapterProm.bothback (err) ->
        firePromise.resolve null, json
    return firePromise

  _findFire: (promise) ->
    {source, ns, fields} = DataSkema = @schema
    source.adapter.find ns, @_conditions, {}, (err, results) ->
      return promise.resolve err      if err
      return promise.resolve null, [] unless results.length
      results = (DataSkema.castObj json for json in results)
      promise.resolve null, results
    return promise

DataQuery::constructor = DataQuery
