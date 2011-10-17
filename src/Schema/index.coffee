# var User = Schema.extend('User', 'users', {
#   name: {
#     first: String,
#     last: String
#   },
#   friends: [User],
#   bestFriend: User,
#   group: Group
# }, {
#   mode: 'stm'
# })
#
# User.source Mongo,
#   name: Mongo.default
#
# User.source Mongo,
#   name: true
#   friends: [DbRef()]
#   bestFriend: DbRef()
#   blogPosts: [DbRef(inverse: 'author')]
#
# BlogPost.source Mongo,
#   author: DbRef()
#   title: String
#
# # Example 3
# User.source Mongo,
#   blogPosts: [DbRef(inverse: 'authors')]
#   blogPosts: [ObjectId]
#   blogPosts: [inverse(BlogPost.authors)]
#   blogPosts: [inverse(BlogPost.authors.id == this.id)]
#
# BlogPost.source Mongo,
#   authors: [DbRef('blogPosts')]
#
# model.get 'users.1.name'
#   # 1. First, lookup the 
#
# User.find(1).name
# User.get('1.name')
#
#
# Via model.subscribe
#
# 1. Subscribe pulls down the object graph from multiple data sources
#    model.subscribe 'path.*', 'path.refA', 'path.refB'
#    model.subscribe ->
#     Room
#       .where('name', params.room)
#       .select([
#         '*',
#         'refA',
#         'refB'
#       ])
#       .findOne()
#
# 2. Map namespace prefix to the pre-configured Schema
# 3. Use the schema + query params to generate 1+ adapter queries
#
#
# Via store.mutator or store.get
#
# 1. Map path -> namespace prefix + query params
# 2. Map namespace -> schema
# 3. f(schema, query) -> adapters + adapter query params
# 4. User schema + query params to generate 1+ adapter queries
# 5. Assemble the data, and pass it to the callback (or create a Promise)
#
# Via a series of store.mutators and store.gets via model.atomic
#
# 1. For each operation, ...
#

# # Supporting dynamic and schema-based approaches at the same time
# ############################################################################
#
# # (1) Just dynamic api; No schemas
# ############################################################################
# model.set '_group.todoList', model.arrayRef '_group.todos', '_group.todoIds'
# # is: model.set '_group.todoList', {$r: '_group.todos', $k: '_group.todoIds'}
#
# # (2) When we move to Memory Schemas
# ####################################
# Group = Schema.extend 'Group', 'groups',
#   name: String
#   todoList: [Todo]
#   topTodo: Todo
#   mySet: Set(Todo)
#
# Todo = Schema.extend 'Todo', 'groups.*.todos',
#   id: Number
#   completed: Boolean
#   text: String
#
# Group.source MemoryAsync,
#   name: String
#   todoList: arrayRef('todos', 'todoIds')
#   todos: Set(Todo)
#   todoIds: [Number]
#
# Todo.source MemoryAsync
#
# # (3) When we move to MySQL
# ###########################
# Group.source MySql,
#   name: pkey
#   todoList: [inverse(Todo, 'group_name')]
#
# Group.source(MySql).index 'field1', 'field2'
#
# Todo.source MySql,
#   id: pkey
#   completed: Boolean
#   text: String
#   group_name: String # foreign key to Group I belong to
#
# # (A) Potential implementation solution
# #######################################
# model.set '_group.todoList', model.arrayRef '_group.todos', '_group.todoIds'
# # This could define a generated, hidden source schema for Group and Todo.
# # arrayRef implies 2 different Schemas (1 for _group.todos and 1 for _group)
#
# # set could behave differently when the value is arrayRef(...) or ref(...),
# # so that we do not write refs data verbatim to a different persistence store
#
# model.push '_group.todoList', id: id, completed: false, text: text
#
# # Creates txn/op - [ver, txnId, 'push', 'groups.stanford.todoList', {id: id, completed: false, text: text}]
# # This op gets sent to the server Store

# TODO Re-name @_doc and @_fields for less confusion

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
  SubClass = (attrs, @isNew = true) ->
    # Instead of a dirty tracking object, we keep an oplog,
    # which we can leverage better at the Adapter layer
    # - e.g., collapse >1 same-path pushes into 1 push for Mongo
    @oplog = []
    @_doc = {}

    # Invoke parent constructors
    ParentClass.apply @, arguments

    if attrs
      for attrName, attrVal of attrs
        # TODO Lazy casting later?
        field = SubClass._fields[attrName]

        # Cast defined fields; ad hoc fields skip this
        if field
          attrVal = field.cast attrVal
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

  SubClass.name = name
  SubClass.namespace = namespace

  SubClass._subclasses = []
  SubClass._superclass = @
  @_subclasses.push SubClass

  # Copy over base static methods
  for static in ['extend', 'static']
    SubClass[static] = Schema[static]

  # Copy over all dynamically generated static methods
  SubClass._statics = {}
  for name, fn of @_statics
    SubClass.static name, fn

  SubClass._fields = {}
  SubClass.field = (fieldName, setToField) ->
    return field if field = @_fields[fieldName]
    @_fields[fieldName] = setToField

  # TODO Add in Harmony Proxy server-side to use a[path] vs a.get(path)
  for fieldName, descriptor of config
    field = Schema.inferType descriptor, fieldName
    SubClass.field fieldName, field

  SubClass.cast = (val) ->
    if val.constructor == Object
      return new @ val
    if val instanceof @
      return val
    throw new Error 'val is neither an Object nor a ' + @::name

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

# TODO Setup method and data structure that is used to
# define async flow control for reads/writes
Schema.static
  _sources: []
  source: (source, ns, fieldsConfig) ->
    @_sources.push source
    source.schemas[ns] = @
    for field, descriptor of fieldsConfig
      # Setup handlers in the data source
      source.addField @, field, descriptor
  fromPath: (path) ->
    pivot = path.indexOf '.'
    namespace = path.substring 0, pivot
    path = path.substring pivot+1
    return { path, schema: @_schemas[namespace] }

  # Send the oplog to the data sources
  applyOps: (oplog, callback, doc) ->
    sources = @_sources
    remainingSources = sources.length
    for source in sources
      # Send oplog to all data sources. Data sources can choose to ignore 
      # the query if it's not relevant to it, or it can choose to 
      # execute the oplog selectively. How does this fit in with STM?
      # We need to have a rollback mechanism
      source.applyOps oplog, (err, extraAttrs) =>
        --remainingSources
        return callback err if err
        if extraAttrs
          for attrName, attrVal of extraAttrs
            doc._doc[attrName] = attrVal
        unless remainingSources
          callback err, doc

  create: (attrs, callback) ->
    obj = new @(attrs)
    obj.save callback

  update: (conds, attrs, callback) ->
    oplog = ([@namespace, conds, 'set', path, val] for path, val of attrs)
    @applyOps oplog, callback

  destroy: (conds, callback) ->
    oplog = [ [@namespace, conds] ]
    @applyOps oplog, callback

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
  _assignAttrs: (name, val, obj = @_doc) ->
    Skema = @constructor
    if val.constructor == Object
      field = Skema._fields[name]
      type = field.type
      if 'function' == typeof type
        # If we have a constructor, most likely Schema
        obj[name] = new type val
        return
      for k, v of val
        nextObj = obj[name] ||= {}
        @_assignAttrs k, v, nextObj
    else if val instanceof Schema
      field = Skema._fields[name]
      type = field.type
      unless val instanceof type
        throw new Error "Trying to assign #{val} to a #{type.name} attribute"
      obj[name] = val
    else if Array.isArray val
      field = Skema._fields[name]
      obj[name] = field.cast val
    else
      obj[name] = val
    return

  atomic: ->
    obj = Object.create @
    obj._atomic = true
    return obj

  set: (attr, val, callback) ->
    @_assignAttrs attr, val
    conds = {_id} if _id = @_doc._id
    @oplog.push [@constructor.namespace, conds, 'set', attr, val]
    if @_atomic
      @save callback
    return @

  # Get from in-memory local @_doc
  # TODO Leverage defineProperty or Proxy.create server-side
  get: (attr) ->
    return @_doc[attr]

  del: (attr, callback) ->
    conds = {_id} if _id = @_doc._id
    @oplog.push [@constructor.namespace, conds, 'del', attr]
    if @_atomic
      @save callback
    return @

  # self-destruct
  destroy: (callback) ->
    conds = {_id} if _id = @_doc._id
    @oplog.push [@constructor.namespace, conds, 'destroy']
    @constructor.applyOps oplog, callback

  push: (attr, vals..., callback) ->
    if 'function' != typeof callback
      vals.push callback
      callback = null
    arr = @_doc[attr] ||= []

    # TODO DRY - This same code apperas in _assignAttrs
    # TODO In fact, this may already be part of Field::cast
    field = @constructor._fields[attr]
    vals = field.cast vals

    arr.push vals...
    conds = {_id} if _id = @_doc._id
    @oplog.push [@constructor.namespace, conds, 'push', attr, vals...]
    if @_atomic
      @save callback
    return @

  # @param {Function} callback(err, document)
  save: (callback) ->
    oplog = @oplog
    @oplog = []
    Skema = @constructor
    Skema.applyOps oplog, callback, @

  validate: ->
    errors = []
    for fieldName, field of @constructor._fields
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
      if schema.name == schemaAsString
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
