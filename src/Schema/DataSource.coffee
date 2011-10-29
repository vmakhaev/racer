{merge} = require '../util'
Promise = require 'Promise'

# Custom DataSource classes are defined via:
# DataSource.extend({
#   set: function (...) {...},
#   del: function (...) {...}
# })
DataSource = module.exports = (@adapter) ->
  # Encapsulates the Data Source schemas for each logical Schema that
  # declared this Data Source as one of its sources.
  # Maps namespace -> fieldName -> field
  @dataSchemas = {}

  @schemas = {} # Maps namespace -> CustomSchema (i.e., logical schema)
  @adapter ||= new AdapterClass() if AdapterClass = @AdapterClass
  return

DataSource:: =
  connect: (config, callback) -> @adapter.connect config, callback

  disconnect: (callback) -> @adapter.disconnect callback

  flush: (callback) -> @adapter.flush callback

  # Shortcut method for use in data source schemas
  # to generate a descriptor specifying a field's
  # type and the fact that it is as a primary key.
  # e.g.,
  #     CustomSchema.source(mongo, ns, {
  #       _id: mongo.pkey(ObjectId)
  #     });
  pkey: (fieldNameOrType) ->
    if 'string' == typeof fieldNameOrType
      return @_pkeyField = fieldNameOrType
    return {
      $type: fieldNameOrType
      $pkey: true
    }

  # @param {Function} the custom LogicalSchema subclass
  # @param {Object} conf maps field names to type descriptor; a
  #     type descriptor can be any number of syntactic representations
  #     of the type the field is.
  createDataSchema: (LogicalSkema, ns, conf) ->
    {_name} = LogicalSkema

    unless conf
      conf = ns
      {ns} = LogicalSkema

    shortcut = @[LogicalSkema._name] = {}
    fields = @dataSchemas[ns] = {}
    for fieldName, descriptor of conf
      # Add field to the data schema
      dataField = fields[fieldName] = @inferType descriptor
      dataField.path = fieldName
      dataField.ns = ns
      # TODO Modify this when we implement non-mirroring field names
      dataField.logicalField = LogicalSkema.fields[fieldName]

      # TODO This must change when there is not a 1-1 mapping between
      #      logical field names and the data field names they correspond to
      LogicalSkema.fields[fieldName].dataFields.push dataField

      # Add a shortcut
      # `shortcut` is for use in rvalues of other data source schemas
      # e.g.,
      #     CustomSchema.source(mongo, 'namespace', {
      #       _id: ObjectId
      #       someAttr: mysql.Skema.id
      #     });
      shortcut[fieldName] =
        $foreignField: dataField # = {source: @, name: fieldName}

    return fields

  # TODO addDataField ?

  # Adds defaults specified in the data source schema
  # to the Logical Source schema document.
  #
  # @param {Schema} document
  addDefaults: (document) ->
    fields = @dataSchemas[document.constructor.ns]
    for fieldName, {defaultTo} of fields
      continue if fieldName == '_id' # TODO Replace _id w/generic pkey
      continue if defaultTo is undefined
      val = document.get fieldName
      if val is undefined
        defaultTo = defaultTo val if 'function' == typeof defaultTo
        # TODO Ensure fieldName is part of logical schema AND data source schema; not part of data source schema but not logical schema
        document.set fieldName, defaultTo


  findOne: (ns, conditions, fields, callback) ->
    sourceProm = new Promise
    sourceProm.bothback callback if callback
    conditions = @_castObj @dataSchemas[ns], conditions

      # 2. Determine if we should generate any other queries
      #    e.g., for queries that include a Ref
      #    QueryDispatcher?

    @adapter.findOne ns, conditions, {}, (err, json) ->
      return sourceProm.resolve err if err
      return sourceProm.resolve null, null unless json

      # TODO Should we have a separate Data Source Schema document, distinguishable from the Logical Schema?
      derefPromises = []
      for path, val of json
        resField = fields[path]
        if resField.deref # Ducktyped @deref
          adapterProm = resField.deref val, (err, dereffedJson) ->
            # Cast using the referenced data source schema
            json[path] = resField.cast dereffedJson
          derefPromises.push adapterProm
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

  find: (ns, conditions, fields, callback) ->
    sourceProm = new Promise
    sourceProm.bothback callback if callback
    conditions = @_castObj @dataSchemas[ns], conditions
    fields = @dataSchemas[ns]
    self = this
    @adapter.find ns, conditions, {}, (err, array) ->
      return sourceProm.resolve err if err
      return sourceProm.resolve null, [] unless array.length
      # TODO Should we have a separate Data Source Schema document, distinguishable from the Logical Schema?
      arr = []
      for json in array
        arr.push self._castObj dataSchema, json
      return sourceProm.resolve null, arr
    return sourceProm

  _castObj: (dataSchema, obj) ->
    for path, val of obj
      field = dataSchema[path]
      obj[path] = field.cast val if field.cast
    return obj

DataSource.extend = (config) ->
  ParentSource = @
  ChildSource = ->
    ParentSource.apply @, arguments
    return

  ChildSource:: = new ParentSource
  ChildSource::constructor = ChildSource

  merge ChildSource::, config

  ChildSource.extend = DataSource.extend

  return ChildSource
