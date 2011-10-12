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
#   name: Mongo.default
#   friends: [ref(User)]
#   bestFriend: ref(User)
#
# User.source Mongo, (mongo) ->
#   mongo.cover 'name'
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

# This is how we define our logical schema (vs our data source schema).
# At this layer, we take care of validation, most typecasting, virtual
# attributes, and methods encapsulating business logic.
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

  SubClass:: = prototype = new @()
  prototype.constructor = SubClass
  prototype.name = name
  prototype.namesapce = namespace

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

# TODO Setup method and data structure that is used to
# define async flow control for reads/writes
Schema.static
  _sources: []
  source: (Source, fieldsConfig) ->
    adapter = new Source
    @_sources.push adapter
    for field, config of fieldsConfig
      # Setup handlers in adapter
      adapter.setup field, config
  fromPath: (path) ->
    pivot = path.indexOf '.'
    namespace = path.substring 0, pivot
    path = path.substring pivot+1
    return { path, schema: @_schemas[namespace] }

  applyOps: (oplog, callback) ->
    sources = @_sources
    remainingSources = sources.length
    for source in sources
      # Send oplog to all adapters. Adapters can choose to ignore 
      # the query if it's not relevant to it, or it can choose to 
      # execute the oplog selectively. How does this fit in with STM?
      # We need to have a rollback mechanism
      source.applyOps oplog, ->
        --remainingSources || callback()

  create: (attrs, callback) ->
    obj = new @(attrs)
    obj.save callback

  update: (conds, attrs, callback) ->
    oplog = ([conds, path, 'set', val] for path, val of attrs)
    @applyOps oplog, callback

  destroy: (conds, callback) ->
    oplog = [ [conds] ]
    @applyOps oplog, callback

  findById: (id, callback) ->
    query = { conds: {id: id}, meta: '*' }
    @query query, callback

  query: (query, callback) ->
    # Compile query into a set of adapter queries
    # with the proper async flow control.

# Copy over where, find, findOne, etc from Query::,
# so we can do e.g., Schema.find, Schema.findOne, etc
for queryMethodName, queryFn of Query::
  do (queryFn) ->
    Schema.static queryMethodName, ->
      query = new Query
      queryFn.apply query, arguments
      return query

Schema:: =
  atomic: ->
    obj = Object.create @
    obj._atomic = true
    return obj

  set: (attr, val, callback) ->
    conds = {_id} if _id = @attrs._id
    @oplog.push [conds, 'set', attr, val]
    if @_atomic
      @save callback
    return @

  # Get from in-memory local @attrs
  # TODO Leverage defineProperty or Proxy.create server-side
  get: (attr) ->
    return @attrs[attr]

  del: (attr, callback) ->
    conds = {_id} if _id = @attrs._id
    @oplog.push [conds, 'del', attr]
    if @_atomic
      @save callback
    return @

  # self-destruct
  destroy: (callback) ->
    conds = {_id} if _id = @attrs._id
    @oplog.push [conds, 'destroy']
    @constructor.applyOps oplog, callback

  save: (callback) ->
    oplog = @oplog
    @oplog = []
    @constructor.applyOps oplog, callback

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
