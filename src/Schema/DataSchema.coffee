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
    logicalPath  = dataPathToLogicalPath[fieldName] || fieldName
    logicalField = LogicalSkema?.fields[logicalPath]
    dataField    = @_createFieldFrom descriptor, logicalField, ns, fieldName

    bootstrapField = (dataField, fieldName, fields, LogicalSkema, logicalPath) ->
      fields[fieldName] = dataField
      if LogicalSkema
        # TODO Place console.warn here? i.e., if dataField is undefined? See console.warn in DSQueryDispatcher
        LogicalSkema.fields[logicalPath].dataFields.push dataField

    if dataField instanceof Promise
      source = @source
      do (fieldName, logicalPath, dataField) ->
        dataField.callback (DataSkema) ->
          conf = {path: fieldName, ns, logicalField, source}
          dataField = DataSkema.createField conf
          bootstrapField dataField, fieldName, fields, LogicalSkema, logicalPath
    else
      bootstrapField dataField, fieldName, fields, LogicalSkema, logicalPath

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
        defaultTo = defaultTo val if typeof defaultTo is 'function'
        # TODO Ensure fieldName is part of logical schema AND data source schema; not part of data source schema but not logical schema
        document.set fieldName, defaultTo

  # When acting like a type
  createField: (opts) -> new DataField @, opts
  uncast: (val) ->
    fields = @fields
    for path, v of val
      field = fields[path]
      if field.type.uncast
        val[path] = field.type.uncast v
    return val

  castObj: (obj) ->
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
    return conf unless type = source.inferType descriptor
    if type instanceof Promise
      return type
    return type.createField conf

  # TODO DRY - repeated in Mongo/types in baseType
  handleSet: (cmd, cmdSet, path, val) ->
    val = @cast val if @cast
    switch cmd.method
      when 'update'
        set = cmd.val.$set ||= {}
        set[path] = val
      when 'insert'
        if -1 == path.indexOf '.'
          cmd.val[path] = val
        else
          @_assignToUnflattened cmd.val, path, val
      else
        throw new Error 'Implement for other incoming method ' + cmd.method
    return true

DataQuery = require './DataQuery'
for queryMethodName, queryFn of DataQuery::
  do (queryFn) ->
    DataSchema::[queryMethodName] = (args...)->
      query = new DataQuery @
      return queryFn.apply query, args

# Used to generate DataSchema.Buffer, which is used in DataSource::schema(schemaName) to buffer up methods invoked on an instance of DataSchema that has not yet been defined
bufferify = (Klass) ->
  klassProto = Klass::
  BufferKlass = -> return
  bufferKlassProto = BufferKlass::
  buffer = []
  for k, v of klassProto
    continue unless typeof v is 'function'
    do (v) ->
      bufferKlassProto[k] = (args...) ->
        buffer.push [v, args]
        return @
  if 'flush' of klassProto && typeof klassProto.flush is 'function'
    throw new Error 'Trying to over-write method `flush`'
  bufferKlassProto.flush = (klassInstance) ->
    for [fn, args] in buffer
      fn.apply klassInstance, args
    return
  return BufferKlass

DataSchema.Buffer = bufferify DataSchema
