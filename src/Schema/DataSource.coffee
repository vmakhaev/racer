{merge} = require '../util'

DataSource = module.exports = (@adapter) ->
  # Encapsulates the Data Source schemas for each logical Schema that
  # declared this Data Source as one of its sources.
  # Maps namespace -> fieldName -> field
  @fields = {}

  @schemas = {} # Maps namespace -> CustomSchema
  @adapter ||= new @AdapterClass() if @AdapterClass
  return

DataSource::=
  connect: (config, callback) -> @adapter.connect config, callback
  disconnect: (callback) -> @adapter.disconnect callback
  flush: (callback) -> @adapter.flush callback

  pkey: (fieldNameOrType) ->
    if 'string' == typeof fieldNameOrType
      return @_pkeyField = fieldNameOrType
    return {
      $type: fieldNameOrType
      $pkey: true
    }
  addField: (Skema, fieldName, descriptor) ->
        
    namespace = Skema.namespace
    nsFields = @fields[namespace] ||= {}
    type = nsFields[fieldName] = @inferType descriptor

    # The following is for use in rvalues of other data source schemas
    # e.g.,
    #     CustomSchema.source(mongo, 'namespace', {
    #       _id: ObjectId
    #       someAttr: thisSrc.Skema._id
    #     });
    @[Skema._name] ||= {}
    @[Skema._name][fieldName] =
      $dataField: # TODO Rename to $foreignField ?
        source: @
        fieldName: fieldName
        type: type

  # Adds defaults specified in the data source schema
  # to the Logical Source schema document.
  #
  # @param {Schema} document
  addDefaults: (document) ->
    ns = document.constructor.namespace
    fields = @fields[ns]
    for fieldName, {defaultTo} of fields
      continue if fieldName == '_id'
      val = document.get fieldName
      if val is undefined && defaultTo isnt undefined
        defaultTo = defaultTo val if 'function' == typeof defaultTo
        # TODO Ensure fieldName is part of logical schema AND data source schema; not part of data source schema but not logical schema
        document.set fieldName, defaultTo

  # @param {Array} oplog
  # @param {Function} callback(err, extraAttrs)
  applyOps: (oplog, callback) ->
    oplog = @_minifyOps oplog if @_minifyOps

    commandSet = @_oplogToCommandSet oplog
    return commandSet.fire @, (err) ->
      return callback err if err
      callback null

  findOne: (ns, conditions, callback) ->
    nsFields = @fields[ns]
    # 1. Cast the query conditions
    for path, val of conditions
      condField = nsFields[path]
      conditions[path] = condField.cast val if condField.cast
    # 2. Dispatch the query to the data adapter
    return @adapter.findOne ns, conditions, {}, (err, json) ->
      return callback err if err
      return callback null, null unless json
      # TODO Should we have a separate Data Source Schema document, distinguishable from the Logical Schema?
      for path, val of json
        resField = nsFields[path]
        json[path] = resField.cast val if resField.cast
      callback null, json

  find: (ns, conditions, callback) ->
    nsFields = @fields[ns]
    # 1. Cast the query conditions
    for path, val of conditions
      condField = nsFields[path]
      conditions[path] = condField.cast val if condField.cast
    # 2. Dispatch the query to the data adapter
    return @adapter.find ns, conditions, {}, (err, array) ->
      return callback err if err
      return callback null, [] unless array.length
      # TODO Should we have a separate Data Source Schema document, distinguishable from the Logical Schema?
      arr = []
      for json in array
        for path, val of json
          resField = nsFields[path]
          json[path] = resField.cast val if resField.cast
        arr.push json
      return callback null, arr

DataSource.extend = (config) ->
  ParentSource = @
  ChildSource = ->
    ParentSource.apply @, arguments
    return

  ChildSource:: = new @
  ChildSource::constructor = ChildSource

  merge ChildSource::, config

  ChildSource.extend = DataSource.extend

  return ChildSource
