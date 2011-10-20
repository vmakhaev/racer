url = require 'url'
mongo = require 'mongodb'
NativeObjectId = mongo.BSONPure.ObjectID
{EventEmitter} = require 'events'

DISCONNECTED  = 1
CONNECTING    = 2
CONNECTED     = 3
DISCONNECTING = 4

# Examples:
# new MongoAdapter 'mongodb://localhost:port/database'
# new MongoAdapter
#   host: 'localhost'
#   port: 27017
#   database: 'example'
MongoAdapter = module.exports = (conf) ->
  @nextId = nextId++
  EventEmitter.call @
  @_loadConf conf if conf
  @_state = DISCONNECTED
  @_collections = {}
  @_pending = []
  return

MongoAdapter:: =
  __proto__: EventEmitter::

  _loadConf: (conf) ->
    if typeof conf == 'string'
      uri = url.parse conf
      @_host = uri.hostname
      @_port = uri.port || 27017
      # TODO callback
      @_database = uri.pathname.replace /\//g, ''
      [@_user, @_pass] = uri.auth?.split(':') ? []
    else
      {@_host, @_port, @_database, @_user, @_pass} = conf

  connect: (conf, callback) ->
    if 'function' == typeof conf
      callback = conf
    else
      @_loadConf conf
    @_db ||= new mongo.Db(
        @_database
      , new mongo.Server @_host, @_port
    )
    @_state = CONNECTING
    @emit 'connecting'
    @_db.open (err) =>
      return callback err if err && callback
      open = =>
        @_state = CONNECTED
        @emit 'connected'
        for [method, args] in @_pending
          @[method].apply @, args
        @_pending = []

      if @_user && @_pass
        return @_db.authenticate @_user, @_pass, open
      return open()

  disconnect: (callback) ->
    collection._ready = false for _, collection of @_collections

    switch @_state
      when DISCONNECTED then callback null
      when CONNECTING then @once 'connected', => @close callback
      when CONNECTED
        @_state = DISCONNECTING
        @_db.close()
        @_state = DISCONNECTED
        @emit 'disconnected'
        # TODO onClose callbacks for collections
        callback() if callback
      when DISCONNECTING then @once 'disconnected', => callback null
  
  flush: (callback) ->
    return @_pending.push ['flush', arguments] if @_state != CONNECTED
    @_db.dropDatabase (err, done) ->
      throw err if err
      callback()

  # Mutator methods called via CustomDataSource::applyOps
  update: (collection, conds, op, opts, callback) ->
    @_collection(collection).update conds, op, opts, callback

  insert: (collection, json, opts, callback) ->
    # TODO Leverage pkey flag; it may not be _id
    json._id = new NativeObjectId
    @_collection(collection).insert json, opts, (err) ->
      return callback err if err
      callback null, {_id: json._id}

  # Callback here receives raw json data back from Mongo
  findOne: (collection, conds, opts, callback) ->
    @_collection(collection).findOne conds, opts, callback

  find: (collection, conds, opts, callback) ->
    @_collection(collection).find conds, opts, (err, cursor) ->
      return callback err if err
      cursor.toArray (err, docs) ->
        return callback err if err
        return callback null, docs

  # Finds or creates the Mongo collection
  _collection: (name) ->
    @_collections[name] ||= new Collection name, @_db


MongoCollection = mongo.Collection

nextId = 1
Collection = (name, db) ->
  self = this
  self.name = name
  self.db = db
  self._pending = []
  self._ready = false

  db.collection name, (err, collection) ->
    throw err if err
    self._ready = true
    self.collection = collection
    self.onReady()

  return

Collection:: =
  onReady: ->
    for todo in @_pending
      @[todo[0]].apply @, todo[1]
    @_pending = []

for name, fn of MongoCollection::
  do (name, fn) ->
    Collection::[name] = ->
      collection = @collection
      args = arguments
      if @_ready
        process.nextTick ->
          collection[name].apply collection, args
      else
        @_pending.push [name, arguments]
