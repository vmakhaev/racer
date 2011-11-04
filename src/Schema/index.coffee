# TODO Re-name @_doc and @fields for less confusion
Promise = require '../Promise'
{EventEmitter} = require 'events'
{merge} = require '../util'
FlowBuilder = require './FlowBuilder'
CommandSet = require './CommandSet'

# This is how we define our logical schema (vs our data source schema).
# At this layer, we take care of validation, most typecasting, virtual
# attributes, and methods encapsulating business logic.
Schema = module.exports = ->
  EventEmitter.call @
  return

Schema._schemas = {}
Schema._subclasses = []
Schema._sources = {}
Schema.extend = (name, ns, config) ->
  ParentClass = @
  # Constructor
  # @param {String -> Object} attrs maps path names to their values
  # @param {Boolean} isNew; will be false when populating this from the
  #                  results of a Query
  # @param {Array} oplog, log of operations. Instead of a dirty tracking
  #     object, we keep an oplog, which we can leverage better at the Adapter
  #     layer - e.g., collapse > 1 same-path pushes into 1 push for Mongo
  SubClass = (attrs, @isNew = true, @oplog = Schema.oplog || []) ->
    @oplog.reset ||= -> @length = 0
    @oplog.nextCid ||= 1
    @cid = @oplog.nextCid++ if @isNew

    @_doc = {}

    ParentClass.apply @, Array::slice.call arguments

    if attrs
      # TODO attrs may contain nested objects
      for attrName, attrVal of attrs
        # TODO Lazy casting later?
        field = SubClass.fields[attrName]

        # 1st conditional term: Cast defined fields; ad hoc fields skip this
        # 2nd conditional term: Don't cast undefineds
        if field && attrVal
          attrVal = field.cast attrVal, if @isNew then @oplog else null
        if @isNew
          @set attrName, attrVal
        else
          @_assignAttrs attrName, attrVal
    # TODO Add the following block back
#     if @isNew
#       # TODO Move source defaults out of constructor?
#       dataSchemas = SubClass.dataSchemas
#       for dataSchema in dataSchemas
#         dataSchema.addDefaults @
    return

  SubClass:: = proto = new ParentClass()
  proto.constructor = SubClass

  # SubClass.name is frozen to ""
  SubClass._name = name
  SubClass.ns = ns

  SubClass._subclasses = []
  SubClass._superclass = @
  @_subclasses.push SubClass

  # Copy over base static methods
  for static in ['extend', 'static']
    SubClass[static] = Schema[static]

  # Copy over all dynamically generated static methods
  SubClass._statics = {}
  for staticName, fn of @_statics
    SubClass.static staticName, fn

  SubClass.fields = {}
  SubClass.field = (fieldName, setToField) ->
    return field if field = @fields[fieldName]
    return @fields[fieldName] = setToField

  # TODO Add in Harmony Proxy server-side to use a[path] vs a.get(path)
  for fieldName, descriptor of config
    field = Schema.createFieldFrom descriptor, fieldName
    bootstrapField = (field, fieldName, SubClass) ->
      field.sources = [] # TODO Are we using field.sources?
      field.path    = fieldName
      field.schema  = SubClass
      SubClass.field fieldName, field
    if field instanceof Promise
      field.callback (schema) ->
        field = schema.createField()
        bootstrapField field, fieldName, SubClass
    else
      bootstrapField field, fieldName, SubClass

  SubClass.cast = (val, oplog) ->
    if val.constructor == Object
      return new @ val, true, oplog if oplog
      return new @ val
    return val if val instanceof @
    throw new Error val + ' is neither an Object nor a ' + @_name

  Schema._schemaPromises[name]?.fulfill SubClass

  return Schema._schemas[ns] = SubClass

Schema._statics = {}
Schema.static = (name, fn) ->
  if name.constructor == Object
    for _name, fn of name
      @static _name, fn
    return @
  
  @_statics[name] = @[name] = fn
  # Add to all subclasses
  decorateDescendants = (descendants, name, fn) ->
    for SubClass in descendants
      continue if SubClass._statics[name]
      SubClass[name] = fn
      decorateDescendants SubClass._subclasses, name, fn
  decorateDescendants @_subclasses, name, fn
  return @

Schema.fromPath = (path) ->
  pivot = path.indexOf '.'
  ns    = path.substring 0, pivot
  path  = path.substring pivot+1
  pivot = path.indexOf '.'
  id    = path.substring 0, pivot
  path  = path.substring pivot+1
  return { Skema: @_schemas[ns], id, path }


# Send the oplog to the data sources, where they
# convert the ops into db commands and exec them
#
# @param {Array} oplog
# @param {Function} callback(err, doc)
# @param {Schema} doc that originates the applyOps call
Schema.applyOps = (oplog, callback) ->
  cmdSet = @_oplogToCommandSet oplog
  return cmdSet.fire (err, cid, extraAttrs) ->
    return callback(err || null)

# Keeping this as a separate function makes testing oplog to
# command set possible.
# TODO This should be able to handle more refined write flow control
Schema._oplogToCommandSet = (oplog) ->
  cmdSet = new CommandSet
  for op in oplog
    {doc, ns, conds, method, path, args} = operation.splat op
    # TODO Handle nested paths
    LogicalSkema = Schema._schemas[ns]
    {dataFields} = logicalField = LogicalSkema.fields[path]

    # TODO How does this fit in with STM? We need a rollback mechanism
    for dataField in dataFields
      {source} = dataField
      source[method] cmdSet, doc, dataField, conds, args...
  return cmdSet

Schema.static
  # TODO Do we even need dataSchemas as a static here?
  dataSchemas: []
  # We use this to define a "data source schema" and link it to
  # this "logical Schema".
  createDataSchema: (source, ns, fieldsConf, virtualsConf) ->
    # (source, fieldsConf)
    if arguments.length == 2
      virtualsConf = null
      fieldsConf = ns
      ns = @ns # Inherit data schema ns from logical schema

    if arguments.length == 3
      switch typeof ns
        when 'boolean', 'string'
          # (source, ns, fieldsConf)
          virtualsConf = null
        else
          # (source, fieldsConf, virtualsConf)
          virtualsConf = fieldsConf
          fieldsConf = ns
          ns = @ns # Inherit data schema ns from logical schema

    # else
    # (source, ns, fieldsConf, virtualsConf)

    Schema._sources[source._name] ||= source
    dataSchema = source.createDataSchema {LogicalSchema: @, ns}, fieldsConf
    @dataSchemas.push dataSchema

#      # In case data schema already exists
#      # TODO Implement addDataFields and addVirtualFields
#      for field, descriptor of fieldsConf
#        source.addDataField @, ns, field, fieldsConf
#      for virtual, descriptor of virtualsConf
#        source.addVirtualField @, ns, virtual, descriptor
    
    return dataSchema

  # Interim method used to help transition from non-Schema to
  # Schema-based approach.
  toOplog: (id, method, args) ->
    [ [@constructor.ns, {_id: id}, method, args] ]

  create: (attrs, callback, oplog = Schema.oplog) ->
    obj = new @(attrs, true, oplog)
    obj.save (err) ->
      return callback err if err
      callback null, obj

  update: (conds, attrs, callback) ->
    oplog = ([@ns, conds, 'set', path, val] for path, val of attrs)
    Schema.applyOps oplog, callback

  destroy: (conds, callback) ->
    oplog = [ [@ns, conds] ]
    Schema.applyOps oplog, callback

  plugin: (plugin, opts) ->
    plugin @, opts
    return @

  # defineReadFlow(callback)
  # Invoking with this fn signature will result in defining
  # a fallback read flow for the entire Logical Schema
  #
  # defineReadFlow(field, callback)
  # Otherwise, invoking with this fn signature will result 
  # in defining a read flow for the given fields
  defineReadFlow: (args...) ->
    callback    = args.pop()
    fieldNames  = args
    flowBuilder = new FlowBuilder

    callback flowBuilder
    if fieldNames.length
      fields = @fields
      for name in fieldNames
        fields[name].dataFields.readFlow = flowBuilder.flow
    else
      @readFlow = flowBuilder.flow
    return @

  lookupField: (path) ->
    return [field, ''] if field = @fields[path]
    parts = path.split '.'
    for part in parts
      if subpath
        subpath += '.' + part
      else
        subpath = part
      if Skema = @fields[subpath]
        [field, ownerPathRelToRoot] = Skema.lookupField path.substring(subpath.length + 1)
        return [field, subpath + '.' + ownerPathRelToSkema]
    throw new Error "path '#{path}' does not appear to be reachable from Schema #{@_name}"

  # TODO This is duplicated in DataSchema
  castObj: (obj) ->
    fields = @fields
    for path, val of obj
      field = fields[path]
      obj[path] = field.cast val if field.cast
    return obj

# Copy over where, find, findOne, etc from Query::,
# so we can do e.g., Schema.find, Schema.findOne, etc
LogicalQuery = require './LogicalQuery'
for queryMethodName, queryFn of LogicalQuery::
  do (queryFn) ->
    Schema.static queryMethodName, (args...)->
      query = new LogicalQuery @
      return queryFn.apply query, args

Schema:: = EventEmitter::
Schema::constructor = Schema
merge Schema::,
  toJSON: -> @_doc

  _assignAttrs: (name, val, obj = @_doc) ->
    {fields, _name} = LogicalSkema = @constructor
    if field = fields[name]
      return obj[name] = field.cast val, if @isNew then @oplog else null
    
    if val.constructor == Object
      for k, v of val
        nextObj = obj[name] ||= {}
        @_assignAttrs k, v, nextObj
      return obj[name]

    throw new Error "Either `#{name}` isn't a field of `#{_name}`, or `#{val}` is not an Object"

  atomic: ->
    return Object.create @,
      _atomic: value: true

  set: (attr, val, callback) ->
    oplogIndex = @oplog.length
    val = @_assignAttrs attr, val
    if _id = @_doc._id
      conds = {_id}
    else
      conds = __cid__: @cid
    if val instanceof Schema
      if fkey = val.get('_id')
        setTo = _id: fkey
      else
        setTo = cid: val.cid
      op = [@, @constructor.ns, conds, 'set', attr, setTo]
      @oplog.splice oplogIndex, 0, op
      # Leaving off a val means assign this attr to the
      # document represented in the next op
    else
      @oplog.push [@, @constructor.ns, conds, 'set', attr, val]
    # Otherwise this operation is stored in val's oplog, since val is a Schema document
    if @_atomic then @save callback
    return @

  # Get from in-memory local @_doc
  # TODO Leverage defineProperty or Proxy.create server-side
  get: (attr) -> return @_doc[attr]

  del: (attr, callback) ->
    if _id = @_doc._id then conds = {_id}
    @oplog.push [@, @constructor.ns, conds, 'del', attr]
    if @_atomic then @save callback
    return @

  # self-destruct
  destroy: (callback) ->
    if _id = @_doc._id then conds = {_id}
    @oplog.push [@, @constructor.ns, conds, 'destroy']
    Schema.applyOps oplog, callback

  push: (attr, vals..., callback) ->
    if typeof callback isnt 'function'
      vals.push callback
      callback = null

    {fields, ns} = LogicalSkema = @constructor
    # TODO DRY - This same code apperas in _assignAttrs
    # TODO In fact, this may already be part of Field::cast
    field = fields[attr]
    vals = field.cast vals, if @isNew then @oplog else null
    arr = @_doc[attr] ||= []
    arr.push vals...

    if _id = @_doc._id
      conds = {_id}
    else
      conds = __cid__: @cid

    if field.type.memberType.prototype instanceof Schema
      setTo = []
      for mem, i in vals
        setTo[i] = cid: mem.cid
      @oplog.splice @oplog.length-vals.length, 0, [@, @constructor.ns, conds, 'push', attr, setTo...]
    else
      @oplog.push [@, ns, conds, 'push', attr, vals...]
    if @_atomic
      @save callback
    return @

  # @param {Function} callback(err, document)
  save: (callback) ->
    oplog = @oplog.slice()
    @oplog.reset()
    self = @
    Schema.applyOps oplog, (err) ->
      return callback err if err
      self.isNew = false
      callback null, self

  validate: ->
    errors = []
    for fieldName, field of @constructor.fields
      result = field.validate @_doc[fieldName]
      continue if result is true
      errors = errors.concat result
    return if errors.length then errors else true


#Schema.async = AsyncSchema
#
#Schema.sync = SyncSchema

Schema.static 'mixin', (mixin) ->
  {init, static, proto} = mixin
  @static static if static
  if proto for k, v of proto
    @::[k] = v
  @_inits.push init if init

contextMixin = require './mixin.context'
Schema.mixin contextMixin

actLikeTypeMixin =
  static:
    setups: []
    validators: []

Type = require './Type'
for methodName, method of Type::
  continue if methodName == 'extend'
  actLikeTypeMixin.static[methodName] = method

Schema.mixin actLikeTypeMixin

# Email = Schema.type 'Email',
#   extend: String
#
# Email.validate (val, callback) ->
#   # ...
#
# Schema.type 'Number',
#   get: (val, doc) -> parseInt val, 10
#   set: (val, doc) -> parseInt val, 10
Schema.type = (typeName, config) ->
  return type if type = @_types[typeName]

  if parentType = config.extend
    config.extend = @type parentType unless parentType instanceof Type
  type = @_types[typeName] = new Type typeName, config

  return type
Schema._types = {}

Schema.createFieldFrom = (descriptor, fieldName) ->
  if descriptor.constructor != Object
    type = @inferType descriptor
    return type if type instanceof Promise
    return type.createField()

  if type = descriptor.$type
    # e.g.,
    # username:
    #   $type: String
    #   validator: fn
    type = @inferType type
    delete descriptor.$type
    # Will be a Promise when the descriptor involves a
    # yet-to-be-defined Schema
    return type if type instanceof Promise
    field = type.createField()
    for method, arg of descriptor
      if Array.isArray arg
        field[method] arg...
      else
        field[method] arg
    return field

# Factory method returning new Field instances
# generated from factory create new Type instances
Schema.inferType = (descriptor) ->
  if Array.isArray descriptor
    arrayType         = @type 'Array'
    concreteArrayType = Object.create arrayType
    memberDescriptor  = descriptor[0]
    memberType        = @inferType memberDescriptor
    if memberType instanceof Promise
      memberType.callback (schema) ->
        concreteArrayType.memberType = schema
    else
      concreteArrayType.memberType = memberType
    return concreteArrayType
  switch descriptor
    when Number  then return @type 'Number'
    when Boolean then return @type 'Boolean'
    when String  then return @type 'String'

  # Allows developers to refer to a LogicalSchema before it's defined
  if typeof descriptor is 'string'
    return @_schemaPromise descriptor

  return descriptor if descriptor           instanceof Type ||
                       descriptor.prototype instanceof Schema

  throw new Error 'Unsupported descriptor ' + descriptor

# We use this when we want to reference a Schema
# that we have not yet defined but that we need for another
# Schema that we are defining at the moment.
# e.g.,
# Schema.extend 'User', 'users',
#   tweets: ['Tweet']
#
# Schema.inferType delegates to the following Schema promise code
Schema._schemaPromises = {}
# @param {String} schemaName
Schema._schemaPromise = (schemaName) ->
  # Cache only one promise per schema
  schemaPromises = @_schemaPromises
  return promise if promise = schemaPromises[schemaName]
  promise = schemaPromises[schemaName] = new Promise
  schemasToDate = Schema._schemas
  for ns, schema of schemasToDate
    if schema._name == schemaName
      promise.fulfill schema
      return promise
  return promise

Schema.type 'String',
  cast: (val) -> val.toString()

Schema.type 'Number',
  cast: (val) -> parseFloat val, 10

Schema.type 'Array',
  cast: (list, oplog) ->
    return (@memberType.cast member, oplog for member in list)

operation =
  splat: ([doc, ns, conds, method, args...]) ->
    {doc, ns, conds, method, path: args[0], args}
