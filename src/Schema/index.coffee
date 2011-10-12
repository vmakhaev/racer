# var User = Schema.extend('User', {
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
#   name: Mongo.default,
#   friends: [ref(User)]
#   bestFriend: ref(User)
#
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

Schema = module.exports = ->
  return

Schema._schemas = {}
Schema._subclasses = []
Schema.extend = (name, namespace, config) ->
  prototype = new @()

  SubClass = (attrs) ->
    # Instead of a dirty tracking object, we keep around an oplog,
    # which we can leverage better at the Adapter layer - e.g., think
    # collapsing multiple pushes into a single push in MongoDB
    @oplog = []

    if attrs for attr, val of attrs
      @set attr, val
    return

  SubClass:: = prototype
  SubClass::constructor = SubClass
  SubClass::name = name
  SubClass::namesapce = namespace

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

Schema.static
  fromPath: (path) ->
    pivot = path.indexOf '.'
    namespace = path.substring 0, pivot
    path = path.substring pivot+1
    return { path, schema: @_schemas[namespace] }

  create: (attrs, callback) ->
    obj = new @(attrs)
    obj.save callback

  findById: (id, callback) ->
    query = { conds: {id: id}, meta: '*' }
    @query query, callback

  find: -> throw new Error 'Unimplemented'
  findOne: -> throw new Error 'Unimplemented'

  query: (query, callback) ->
    # Compile query into a set of adapter queries
    # with the proper async flow control.

Schema:: =
  set: (attr, val) ->
    @oplog.push ['set', attr, val]
    return @

  save: (callback) ->
    @constructor
    # Send oplog to all adapters. Adapters can
    # choose to ignore the query if it's not relevant
    # to it, or it can choose to execute the oplog selectively.
    # How does this fit in with STM? We need to have a rollback
    # mechanism

#Schema.async = AsyncSchema
#
#Schema.sync = SyncSchema

Schema.static 'mixin', (mixin) ->
  {static, proto} = mixin
  @static static if static
  if proto for k, v of proto
    @::[k] = v

contextMixin = require './mixin.context'
Schema.mixin contextMixin
