{merge} = require '../util'
AbstractQuery = require './AbstractQuery'
Promise = require '../Promise'

DataQuery = module.exports = (criteria) ->
  AbstractQuery.call @, criteria
  return

DataQuery:: = merge new AbstractQuery(),
  # Options methods

  # TODO Do something with @_fields
  # @param {String->Field} fields
  fields: (@_fields) ->

  fire: (fireCallback) ->
    conds = @castConditions()

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

    firePromise = (new Promise).bothback fireCallback

    @['_' + @queryMethod + 'Fire'](firePromise)

    return firePromise

  _findOneFire: (firePromise) ->
    {source, ns, fields} = @schema
    source.adapter.findOne ns, @_conditions, {}, (err, json) ->
      return firePromise.fail err if err
      return firePromise.resolve null, null unless json
      derefPromises = []
      # TODO Part of the following block is duplicated in DataSource::castObj
      for path, val of json
        resField = fields[path]
        if resField.deref # Ducktyped @deref
          do (path) ->
            derefProm = resField.deref val, (err, dereffedJson) ->
              # Uncast using the referenced data source schema
  #            console.log dereffedJson
  #            console.log resField
  #            json[path] = resField.type.uncast dereffedJson if resField.type.uncast
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

  _findFire: (firePromise) ->
    {source, ns, fields} = DataSkema = @schema
    source.adapter.find ns, @_conditions, {}, (err, array) ->
      return firePromise.resolve err if err
      return firePromise.resolve null, [] unless array.length
      arr = []
      for json in array
        arr.push DataSkema.castObj json
      return firePromise.resolve null, arr
    return firePromise

DataQuery::constructor = DataQuery
