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
    if type = source.inferType descriptor
      return type.createField conf
    return conf

DataQuery = require './DataQuery'
for queryMethodName, queryFn of DataQuery::
  do (queryFn) ->
    DataSchema::[queryMethodName] = ->
      query = (new DataQuery).bind @
      queryReturn = queryFn.apply query, arguments
      return queryReturn
