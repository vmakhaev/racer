# TODO Re-name @_doc and @fields for less confusion

LogicalQuery = require './LogicalQuery'
Type = require './Type'
Promise = require '../Promise'
{EventEmitter} = require 'events'
{merge} = require '../util'

# This is how we define our logical schema (vs our data source schema).
# At this layer, we take care of validation, most typecasting, virtual
# attributes, and methods encapsulating business logic.
Schema = module.exports = ->
  EventEmitter.call @
  return

Schema._schemas = {}
Schema._subclasses = []
Schema.extend = (name, namespace, config) ->
  ParentClass = @
  # Constructor
  # @param {Object} attrs maps path names to values
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

    # Invoke parent constructors
    ParentClass.apply @, arguments

    if attrs
      for attrName, attrVal of attrs
        # TODO Lazy casting later?
        field = SubClass.fields[attrName]

        # Cast defined fields; ad hoc fields skip this
        if field
          attrVal = field.cast attrVal, if @isNew then @oplog else null
        if @isNew
          # TODO Move source defaults out of constructor?
          sources = SubClass._sources
          for source in sources
            source.addDefaults @
          @set attrName, attrVal
        else
          @_assignAttrs attrName, attrVal
    return

  SubClass:: = prototype = new @()
  prototype.constructor = SubClass

  # SubClass.name is frozen to ""
  SubClass._name = name
  SubClass.namespace = namespace

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
    @fields[fieldName] = setToField

  # TODO Add in Harmony Proxy server-side to use a[path] vs a.get(path)
  for fieldName, descriptor of config
    field = Schema.inferType descriptor, fieldName
    SubClass.field fieldName, field

  SubClass.cast = (val, oplog) ->
    if val.constructor == Object
      return new @ val, true, oplog if oplog
      return new @ val
    if val instanceof @
      return val
    throw new Error val + ' is neither an Object nor a ' + @_name

  return Schema._schemas[namespace] = SubClass

Schema._statics = {}
Schema.static = (name, fn) ->
  if name.constructor == Object
    for static, fn of name
      @static static, fn
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
  namespace = path.substring 0, pivot
  path = path.substring pivot+1
  pivot = path.indexOf '.'
  id = path.substring 0, pivot
  path = path.substring pivot+1
  return { Skema: @_schemas[namespace], id, path }


# Send the oplog to the data sources, where they
# convert the ops into db commands and exec them
#
# @param {Array} oplog
# @param {Function} callback(err, doc)
# @param {Schema} doc that originates the applyOps call
Schema.applyOps = (oplog, callback) ->
  sources = @_sources
  remainingSources = sources.length
  for source in sources
    # TODO Perhaps each source adds to a QuerySet that then gets run
    #      after all sources have acked the oplog. Perhaps we call
    #      the thing that manages this the QueryManager or SourceManager?

    # TODO We may need to aggregate all oplogs from fields that are Schemas
    # when the current Schema doc's oplog does not implicitly carry the
    # oplog of these fields that are Schemas. This may occur, e.g., when
    #     foreignDoc.set foreignPathA, someVal
    #     foreignDoc.push foreignPathB, thisDoc
    #     thisDoc.save callback
    # In this example, we would want save to also save the changes on foreignDoc
    # because it is linked to thisDoc. "Linked" is defined as a connected subgraph
    # of related documents where we follow all edges to documents that have "changed".

    # Send oplog to all data sources. Data sources can choose to ignore 
    # the query if it's not relevant to it, or it can choose to exec 
    # the oplog selectively. How does this fit in with STM?
    # We need to have a rollback mechanism
    source.applyOps oplog, (err, extraAttrs) =>
      # `extraAttrs` are attributes that were not present in the oplog
      # when sent to the source, but that were then created by the source.
      # These new extraAttr need to be written back to the Schema document
      # -- e.g., auto-incrementing primary key in MySQL
      --remainingSources
      return callback err if err
      if extraAttrs
        for attrName, attrVal of extraAttrs
          doc._doc[attrName] = attrVal
      unless remainingSources
        callback err

Schema.static
  _sources: []
  source: (source, ns, fieldsConfig) ->
    Schema._sources.push source
    source.schemas[ns] = @
    for field, descriptor of fieldsConfig
      # Setup handlers in the data source
      source.addField @, field, descriptor

  # Interim method during transition from non-Schema to
  # Schema-based approach.
  toOplog: (id, method, args) ->
    [ [@constructor.namespace, {_id: id}, method, args] ]

  create: (attrs, callback, oplog = Schema.oplog) ->
    obj = new @(attrs, true, oplog)
    obj.save (err) ->
      return callback err if err
      callback null, obj

  update: (conds, attrs, callback) ->
    oplog = ([@namespace, conds, 'set', path, val] for path, val of attrs)
    Schema.applyOps oplog, callback

  destroy: (conds, callback) ->
    oplog = [ [@namespace, conds] ]
    Schema.applyOps oplog, callback

  findById: (id, callback) ->
    query = { conds: {id: id}, meta: '*' }
    @query query, callback

  query: (query, callback) ->
    # Compile query into a set of data source queries
    # with the proper async flow control.
    throw new Error 'Undefined'

  plugin: (plugin, opts) ->
    plugin @, opts
    return @

# Copy over where, find, findOne, etc from Query::,
# so we can do e.g., Schema.find, Schema.findOne, etc
for queryMethodName, queryFn of LogicalQuery::
  do (queryFn) ->
    Schema.static queryMethodName, ->
      query = (new LogicalQuery).bind @
      queryReturn = queryFn.apply query, arguments
      return queryReturn

Schema:: = EventEmitter::
Schema::constructor = Schema
merge Schema::,
  toJSON: -> @_doc

  _assignAttrs: (name, val, obj = @_doc) ->
    Skema = @constructor
    if field = Skema.fields[name]
      return obj[name] = field.cast val, if @isNew then @oplog else null
    
    if val.constructor == Object
      for k, v of val
        nextObj = obj[name] ||= {}
        @_assignAttrs k, v, nextObj
      return obj[name]

    throw new Error "Either #{name} isn't a field of #{Skema._name}, or #{val} is not an Object"

  atomic: ->
    obj = Object.create @
    obj._atomic = true
    return obj

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
      @oplog.splice oplogIndex, 0, [@, @constructor.namespace, conds, 'set', attr, setTo]
      # Leaving off a val means assign this attr to the
      # document represented in the next op
    else
      @oplog.push [@, @constructor.namespace, conds, 'set', attr, val]
    # Otherwise this operation is stored in val's oplog, since val is a Schema document
    if @_atomic
      @save callback
    return @

  # Get from in-memory local @_doc
  # TODO Leverage defineProperty or Proxy.create server-side
  get: (attr) ->
    return @_doc[attr]

  del: (attr, callback) ->
    conds = {_id} if _id = @_doc._id
    @oplog.push [@, @constructor.namespace, conds, 'del', attr]
    if @_atomic
      @save callback
    return @

  # self-destruct
  destroy: (callback) ->
    conds = {_id} if _id = @_doc._id
    @oplog.push [@, @constructor.namespace, conds, 'destroy']
    Schema.applyOps oplog, callback

  push: (attr, vals..., callback) ->
    if 'function' != typeof callback
      vals.push callback
      callback = null
    arr = @_doc[attr] ||= []

    # TODO DRY - This same code apperas in _assignAttrs
    # TODO In fact, this may already be part of Field::cast
    field = @constructor.fields[attr]
    vals = field.cast vals

    arr.push vals...
    if _id = @_doc._id
      conds = {_id}
    else
      conds = __cid__: @cid
    @oplog.push [@, @constructor.namespace, conds, 'push', attr, vals...]
    if @_atomic
      @save callback
    return @

  # @param {Function} callback(err, document)
  save: (callback) ->
    oplog = @oplog.slice()
    @oplog.reset()
    Schema.applyOps oplog, callback

  validate: ->
    errors = []
    for fieldName, field of @constructor.fields
      result = field.validate(@_doc[fieldName])
      continue if true == result
      errors = errors.concat result
    return if errors.length then errors else true

  # We use this when we want to reference a Schema
  # that we have yet to define.
  schema: (schemaAsString) ->
    promise = new Promise
    promise.on (schema) =>
      SubSchema = @_schemas[schemaAsString]
      SubSubSchema = ->
        SubSchema.apply @, arguments
      SubSubSchema:: = new SubSchema
      SubSubSchema.assignAsTypeToSchemaField schema, fieldName
    Schema.on 'define', (schema) ->
      if schema._name == schemaAsString
        promise.fulfill schema, promise.fieldName
    return promise

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

# Factory method returning new Field instances
# generated from factory create new Type instances
Schema.inferType = (descriptor, fieldName) ->
  if descriptor.constructor == Object
    if '$type' of descriptor
      # e.g.,
      # username:
      #   $type: String
      #   validator: fn
      field = Schema.inferType descriptor.$type
      delete descriptor.$type
      for method, arg of descriptor
        if Array.isArray arg
          field[method] arg...
        else
          field[method] arg
        return field

  if Array.isArray descriptor
    arrayType = @type 'Array'
    memberType = descriptor[0]
    concreteArrayType = Object.create arrayType
    concreteArrayType.memberType = @inferType memberType
    return concreteArrayType.createField()
  if descriptor == Number
    return@type('Number').createField()
  if descriptor == Boolean
    return @type('Boolean').createField()
  if descriptor == String
    return @type('String').createField()

  # e.g., descriptor = schema('User')
  if descriptor instanceof Promise
    if @_schemas[fieldName]
      promise.fulfill schema, fieldName
    return descriptor

  # If we're a constructor
  if 'function' == typeof descriptor
    return descriptor.createField()

  if descriptor instanceof Type
    return descriptor.createField()

  throw new Error 'Unsupported descriptor ' + descriptor

Schema.type 'String',
  cast: (val) -> val.toString()

Schema.type 'Number',
  cast: (val) -> parseFloat val, 10

Schema.type 'Array',
  cast: (list) ->
    return (@memberType.cast member for member in list)
