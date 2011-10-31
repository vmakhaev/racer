Promise = require '../Promise'
DataField = require './DataField'

DataSchema = module.exports = (@source, @name, ns, LogicalSkema, conf, logicalPathToDataPath = {}) ->
  unless @ns = ns
    # Disable find, findOne if missing a namespace
    @find = @findOne = null

  # Indexes for converting between logical and data schema
  # path names
  @logicalPathToDataPath = logicalPathToDataPath
  dataPathToLogicalPath = @dataPathToLogicalPath = {}
  for k, v of logicalPathToDataPath
    dataPathToLogicalPath[v] = k

  # Compile the fields
  fields = @fields = {}
  for fieldName, descriptor of conf
    # Add field to the data schema
    logicalPath = dataPathToLogicalPath[fieldName] || fieldName
    dataField = fields[fieldName] = @_createFieldFrom descriptor, LogicalSkema?.fields[logicalPath], ns, fieldName

    if LogicalSkema
      LogicalSkema.fields[logicalPath].dataFields.push dataField
  return

DataSchema:: =
  # shortcut for use in rvalues of other data source schemas
  # e.g.,
  #     CustomSchema.source(mongo, 'namespace', {
  #       _id: ObjectId
  #       someAttr: mysql.Skema.field('id')
  #     });
  field: (path) -> $pointsTo: @fields[path]

  cast: (val) ->
    fields = @fields
    for path, v of val
      type = fields[path].type
      if type.cast
        val[path] = type.cast v
    return val

  # Adds defaults specified in the data source schema
  # to the Logical Source schema document.
  #
  # @param {Schema} document
  addDefaults: (document) ->
    fields = @fields
    for fieldName, {defaultTo} of fields
      # TODO Replace _id w/generic pkey
      continue if fieldName == '_id' || defaultTo is undefined
      val = document.get fieldName
      if val is undefined
        defaultTo = defaultTo val if 'function' == typeof defaultTo
        # TODO Ensure fieldName is part of logical schema AND data source schema; not part of data source schema but not logical schema
        document.set fieldName, defaultTo

  # @param {String} ns is the namespace relative to the data source
  # @param {Object} conds
  # @param {String->Field} fields
  # @param {Function} callback
  findOne: (conds, fields, callback) ->
    sourceProm = new Promise
    sourceProm.bothback callback if callback
    conds = @_castObj conds

      # 2. Determine if we should generate any other queries
      #    e.g., for queries that include a Ref
      #    QueryDispatcher?

    fields = @fields
    @source.adapter.findOne @ns, conds, {}, (err, json) ->
      return sourceProm.resolve err if err
      return sourceProm.resolve null, null unless json

      derefPromises = []
      # TODO Part of the following block is duplicated in DataSource::_castObj
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
          return sourceProm.resolve null, json
        when 1
          adapterProm = derefPromises[0]
        else
          adapterProm = Promise.parallel derefPromises
      return adapterProm.bothback (err) ->
        sourceProm.resolve null, json
    return sourceProm

  # @param {String} ns is the namespace relative to the data source
  # @param {Object} conds
  # @param {String->Field} fields
  # @param {Function} callback
  find: (conds, fields, callback) ->
    sourceProm = new Promise
    sourceProm.bothback callback if callback
    # TODO ns is not always going to match up with logical ns
    conds = @_castObj conds
    self = this
    @adapter.find @ns, conds, {}, (err, array) ->
      return sourceProm.resolve err if err
      return sourceProm.resolve null, [] unless array.length
      # TODO Should we have a separate Data Source Schema document, distinguishable from the Logical Schema?
      arr = []
      for json in array
        arr.push self._castObj json
      return sourceProm.resolve null, arr
    return sourceProm

  # When acting like a type
  createField: (opts) -> new DataField @, opts
  uncast: (val) ->
    fields = @fields
    for path, v of val
      field = fields[path]
      if field.type.uncast
        val[path] = field.type.uncast v
    return val

  _castObj: (obj) ->
    fields = @fields
    for path, val of obj
      field = fields[path]
      obj[path] = field.cast val if field.cast
    return obj

  # TODO addDataField ?

  # @param {Object} descriptor
  # @param {Field} logicalField
  # @param {String|False} ns is the namespace relative to the data source (as
  #     opposed to the logical source schema). `false` means that this is to 
  #     be used for embedded docs
  # @param {String} path
  _createFieldFrom: (descriptor, logicalField, ns, path) ->
    source = @source
    conf = {path, ns, logicalField, source}
    if type = source.inferType descriptor
      return type.createField conf
    return conf
