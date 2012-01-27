redis = require 'redis'
PubSub = require './PubSub'
redisInfo = require './redisInfo'
journal = require './journal'
MemoryAdapter = require './adapters/Memory'
Model = require './Model.server'
transaction = require './transaction'
{split: splitPath, lookup} = require './pathParser'
Promise = require './Promise'
Field = require './mixin.ot/Field.server'
pathParser = require './pathParser'
{bufferify} = require './util'
_query_ = require './query'

# store = new Store
#   mode: 'lww' || 'stm' || 'ot'
#   redis:
#     port: xxxx
#     host: xxxx
#     db: xxxx
#     password: xxxx
Store = module.exports = (options = {}) ->
  self = this

  setupRedis self, options.redis

  @_pubSub = new PubSub
    adapter:
      type: 'Redis'
      pubClient: @_redisClient
      subClient: @_txnSubClient
    onMessage: (clientId, msg) ->
      {txn, ot, rmDoc, addDoc} = msg
      return self._onTxnMsg clientId, txn if txn
      return self._onOtMsg clientId, ot if ot

      # Live Query Channels
      # These following 2 channels are for
      # informing a client about changes to their
      # data set based on mutations that add/rm
      # docs to/from the data set enclosed by the
      # live queries the client subscribes to
      if socket = self._clientSockets[clientId]
        return socket.emit 'rmDoc', rmDoc if rmDoc
        return socket.emit 'addDoc', addDoc if addDoc
      throw new Error 'Unsupported message: ' + JSON.stringify(msg, null, 2)

  # Add a @commit method to this store based on the conflict resolution mode
  @commit = journal.commitFn self, options.mode

  # Maps path -> { listener: fn, queue: [msg], busy: bool }
  # TODO Encapsulate this at a lower level of abstraction
  @_otFields = {}

  @_localModels = {}

  @_adapter = options.adapter || new MemoryAdapter

  # This model is used to create transactions with id's
  # prefixed with '#', so we handle store's async mutations
  # differently than regular models' sync mutations
  @_model = @_createStoreModel()

  @_persistenceRoutes = {}
  @_defaultPersistenceRoutes = {}
  for method in ['get', 'set', 'del', 'setNull', 'incr', 'push',
    'unshift', 'insert', 'pop', 'shift', 'remove', 'move']
    @_persistenceRoutes[method] = []
    @_defaultPersistenceRoutes[method] = []
  @_adapter.setupDefaultPersistenceRoutes this

  return

# TODO Do we even use/need @_model from a Store instance?
Store:: =

  _createStoreModel: ->
    model = @createModel()
    for key, val of model.async
      continue if key == 'get'
      # TODO Beware: mixing in has danger of naming conflicts; better to
      #      delegate to @_model.async instead.
      # TODO Define store mutators here instead of copying from Async because
      #      using Async from here ends up making a hop back to here before
      #      accessing store again (e.g., async.model.store._adapter). Instead
      #      we can be more direct (e.g., @_adapter)
      @[key] = val
    return model

  query: (query, callback) ->
    dbQuery = _query_.deserialize query.serialize(), @_adapter.Query
    self = this
    dbQuery.run @_adapter, (err, found) ->
      # TODO Get version consistency right in face of concurrent writes during
      # query
      if Array.isArray found
        for doc in found
          doc.id = doc._id
          # TODO _id code is specific to Mongo. Move this behind Mongo
          #      abstraction
          delete doc._id
      else
        found.id = found._id
        delete found._id
      callback err, found, self._adapter.version

  get: (path, callback) ->
    @sendToDb 'get', [path], callback

#  set: (path, val, ver, callback) ->
#    # TODO Use @commit here instead? - See mixin.stm/Async
#    @sendToDb 'set', [path, val, ver], callback
#
  createModel: ->
    model = new Model
    model.store = this
    model._ioUri = @_ioUri
    model.startIdPromise = startIdPromise = @_startIdPromise
    startIdPromise.on (startId) ->
      model._startId = startId
    @_redisClient.incr 'clientClock', (err, value) ->
      throw err if err
      clientId = value.toString(36)
      model.clientIdPromise.fulfill clientId
    return model

  flush: (callback) ->
    rem = 2
    cb = (err) ->
      if !(--rem) || err
        callback err if callback
        return callback = null
    @_adapter.flush cb
    self = this
    @_redisClient.flushdb (err) ->
      return cb err if err
      redisInfo.onStart self._redisClient, cb
      self._model = self._createStoreModel()

  disconnect: ->
    @_redisClient.quit()
    @_subClient.quit()
    @_txnSubClient.quit()

  _finishCommit: (txn, ver, callback) ->
    transaction.base txn, ver
    args = transaction.args(txn).slice()
    method = transaction.method txn
    args.push ver
    self = this
    @sendToDb method, args, (err, origDoc) ->
      self._pubSub.publish transaction.path(txn), {txn}, {origDoc}
      callback err, txn if callback

  setSockets: (@sockets, @_ioUri = '') ->
    self = this

    @_clientSockets = clientSockets = {}
    pubSub = @_pubSub
    redisClient = @_redisClient

    # TODO: These are for OT, which is hacky. Clean up
    otFields = @_otFields
    adapter = @_adapter
    hasInvalidVer = @_hasInvalidVer

    # TODO Once socket.io supports query params in the
    # socket.io urls, then we can remove this. Instead,
    # we can add the socket <-> clientId assoc in the
    # `sockets.on 'connection'...` callback.
    sockets.on 'connection', (socket) -> socket.on 'sub', (clientId, targets, ver, clientStartId) ->
      return if hasInvalidVer socket, ver, clientStartId

      # TODO Map the clientId to a nickname (e.g., via session?),
      # and broadcast presence
      socket.on 'disconnect', ->
        pubSub.unsubscribe clientId
        delete clientSockets[clientId]
        redisClient.del 'txnClock.' + clientId, (err, value) ->
          throw err if err

      # TODO WHEN IS THIS CALLED?
      socket.on 'subAdd', (clientId, targets, callback) ->
        for targ, i in targets
          if Array.isArray targ
            # Deserialize targ into a Query instance
            targets[i] = _query_.deserialize targ
        rem = 2
        pubSub.subscribe clientId, targets, (err, data) ->
          # TODO Handle err
          --rem || callback data
        self._fetchSubData targets, (err, data) ->
          # TODO Handle err
          --rem || callback data

      socket.on 'subRemove', (clientId, targets) ->
        throw 'Unimplemented: subRemove'

      # Handling OT messages
      socket.on 'otSnapshot', (setNull, fn) ->
        # Lazy create/snapshot the OT doc
        if field = otFields[path]
          # TODO
          TODO = 'TODO'

      socket.on 'otOp', (msg, fn) ->
        {path, op, v} = msg

        flushViaFieldClient = ->
          unless fieldClient = field.client socket.id
            fieldClient = field.registerSocket socket
            # TODO Cleanup with field.unregisterSocket
          fieldClient.queue.push [msg, fn]
          fieldClient.flush()

        # Lazy create the OT doc
        unless field = otFields[path]
          field = otFields[path] =
            new Field self, pubSub, path, v
          # TODO Replace with sendToDb
          adapter.get path, (err, val, ver) ->
            # Lazy snapshot initialization
            snapshot = field.snapshot = val?.$ot || ''
            flushViaFieldClient()
        else
          flushViaFieldClient()

      # Handling transaction messages
      socket.on 'txn', (txn, clientStartId) ->
        base = transaction.base txn
        return if hasInvalidVer socket, base, clientStartId
        self.commit txn, (err, txn) ->
          txnId = transaction.id txn
          base = transaction.base txn
          # Return errors to client, with the exeption of duplicates, which
          # may need to be sent to the model again
          return socket.emit 'txnErr', err, txnId if err && err != 'duplicate'
          self._nextTxnNum clientId, (num) ->
            socket.emit 'txnOk', txnId, base, num

      socket.on 'txnsSince', (ver, clientStartId, callback) ->
        return if hasInvalidVer socket, ver, clientStartId
        txnsSince pubSub, redisClient, ver, clientId, (txns) ->
          self._nextTxnNum clientId, (num) ->
            if len = txns.length
              socket.__base = transaction.base txns[len - 1]
            callback txns, num

      # This is used to prevent emitting duplicate transactions
      socket.__base = 0
      # Set up subscriptions to the store for the model
      clientSockets[clientId] = socket

      # We guard against the following race condition:
      # Window 1 and Window 2 are both snapshotted at the same ver.
      # Window 1 commits a txn A. Window 1 subscribes.
      # Window 2 subscribes, but before the server can publish the
      # txn A that it missed, Window 1 publishes txn B that
      # is immediately broadcast to Window 2 just before txn A is
      # broadcast to Window 2.
      pubSub.subscribe clientId, targets, ->

  _nextTxnNum: (clientId, callback) ->
    @_redisClient.incr 'txnClock.' + clientId, (err, value) ->
      throw err if err
      callback value

  _onTxnMsg: (clientId, txn) ->
    # Don't send transactions back to the model that created them.
    # On the server, the model directly handles the store.commit callback.
    # Over Socket.io, a 'txnOk' message is sent below.
    return if clientId == transaction.clientId txn
    # For models only present on the server, process the transaction
    # directly in the model
    return model._onTxn txn if model = @_localModels[clientId]
    # Otherwise, send the transaction over Socket.io
    if socket = @_clientSockets[clientId]
      # Prevent sending duplicate transactions by only sending new versions
      base = transaction.base txn
      if base > socket.__base
        socket.__base = base
        @_nextTxnNum clientId, (num) ->
          socket.emit 'txn', txn, num

  _onOtMsg: (clientId, ot) ->
    if socket = @_clientSockets[clientId]
      return if socket.id == ot.meta.src
      socket.emit 'otOp', ot

  registerLocalModel: (model) -> @_localModels[model._clientId] = model

  unregisterLocalModel: (model) -> delete @_localModels[model._clientId]

  # Fetch the set of data represented by `targets` and subscribe to future
  # changes to this set of data.
  # @param {String} clientId representing the subscriber
  # @param {[String|Query]} targets (i.e., paths, or queries) to subscribe to
  # @param {Function} callback(err, data, otData)
  subscribe: (clientId, targets, callback) ->
    rem = 2
    _data = null
    _otData = null
    @_pubSub.subscribe clientId, targets, (err) ->
      --rem || callback err, _data, _otData
    @_fetchSubData targets, (err, data, otData) ->
      _data = data
      _otData = otData
      --rem || callback err, _data, _otData

  unsubscribe: (clientId) -> @_pubSub.unsubscribe clientId

  _fetchSubData: (targets, callback) ->
    data = []
    otData = {}

    finish = ->
      unless --finish.remainingFetches
        callback null, data, otData

    finish.remainingFetches = targets.length

    otFields = @_otFields
    for targ in targets
      self = this
      if targ.isQuery then do (targ) ->
        query = targ
        self.query query, (err, found, ver) ->
          return callback err if err
          queryResultAsDatum = (doc, ver, query) ->
            path = query._namespace + '.' + doc.id
            [path, doc, ver]
          if Array.isArray found
            for doc in found
              data.push queryResultAsDatum(doc, ver, query)
          else
            data.push queryResultAsDatum(found, ver, query)
          finish()
      else
        [root, remainder] = splitPath targ

        @get root, do (root, remainder) -> (err, value, ver) ->
          return callback err if err
          # TODO Make ot field detection more accurate. Should cover all remainder scenarios.
          # TODO Convert the following to work beyond MemoryStore
          otPaths = allOtPaths value, root + '.'
          for otPath in otPaths
            otData[otPath] = otField if otField = otFields[otPath]

          # addSubDatum mutates data argument
          addSubDatum data, root, remainder, value, ver, finish
          finish()
    return

  ## PERSISTENCE ROUTER ##
  route: (method, path, fn) ->
    re = pathParser.eventRegExp path
    @_persistenceRoutes[method].push [re, fn]
    return @

  defaultRoute: (method, path, fn) ->
    re = pathParser.eventRegExp path
    @_defaultPersistenceRoutes[method].push [re, fn]
    return @

  sendToDb:
    bufferify 'sendToDb',
      await: (done) ->
        adapter = @_adapter
        return done() if adapter.version isnt undefined
        @_redisClient.get 'ver', (err, ver) ->
          throw err if err
          adapter.version = parseInt(ver, 10)
          return done()
      origFn: (method, args, done) ->
        perRoutes = @_persistenceRoutes
        defPerRoutes = @_defaultPersistenceRoutes
        routes = perRoutes[method].concat defPerRoutes[method]
        [path, rest...] = args
        done ||= (err) ->
          throw err if err
        i = 0
        do next = ->
          unless handler = routes[i++]
            throw new Error "No persistence handler for #{method}(#{args.join(', ')})"
          [re, fn] = handler
          return next() unless path == '' || (match = path.match re)
          captures = if path == ''
                       ['']
                      else if match.length > 1
                        match[1..]
                      else
                        [match[0]]
          return fn.apply null, captures.concat(rest, [done, next])

txnsSince = (pubSub, redisClient, ver, clientId, callback) ->
  return callback [] unless pubSub.hasSubscriptions clientId

  # TODO Replace with a LUA script that does filtering?
  redisClient.zrangebyscore 'txns', ver, '+inf', 'withscores', (err, vals) ->
    throw err if err
    txn = null
    txns = []
    for val, i in vals
      if i % 2
        continue unless pubSub.subscribedToTxn clientId, txn
        transaction.base txn, +val
        txns.push txn
      else
        txn = JSON.parse val
    callback txns

# Accumulates an array of tuples to set [path, value, ver]
#
# @param {Array} data is an array that gets mutated
# @param {String} root is the part of the path up to ".*"
# @param {String} remainder is the part of the path after "*"
# @param {Object} value is the lookup value of the rooth path
# @param {Number} ver is the lookup ver of the root path
addSubDatum = (data, root, remainder, value, ver, finish) ->
  # Set the entire object
  return data.push [root, value, ver]  unless remainder?

  # Set each property one level down, since the path had a '*'
  # following the current root
  [appendRoot, remainder] = splitPath remainder
  for prop of value
    nextRoot = if root then root + '.' + prop else prop
    nextValue = value[prop]
    if appendRoot
      nextRoot += '.' + appendRoot
      nextValue = lookup appendRoot, nextValue

    addSubDatum data, nextRoot, remainder, nextValue, ver, finish
  return

allOtPaths = (obj, prefix = '') ->
  results = []
  return results unless obj && obj.constructor is Object
  for k, v of obj
    if v && v.constructor is Object
      if v.$ot
        results.push prefix + k
        continue
      results.push allOtPaths(v, k + '.')...
  return results

setupRedis = (self, redisOptions = {}) ->
  {port, host, db, password} = redisOptions
  # Client for data access and event publishing
  self._redisClient = redisClient = redis.createClient(port, host, redisOptions)
  # Client for internal Racer event subscriptions
  self._subClient = subClient = redis.createClient(port, host, redisOptions)
  # Client for event subscriptions of txns only
  self._txnSubClient = txnSubClient = redis.createClient(port, host, redisOptions)
  if password
    authCallback = (err) -> throw err if err
    redisClient.auth password, authCallback
    subClient.auth password, authCallback
    txnSubClient.auth password, authCallback

  # TODO: Make sure there are no weird race conditions here, since we are
  # caching the value of starts and it could potentially be stale when a
  # transaction is received
  # TODO: Make sure this works when redis crashes and is restarted
  redisStarts = null
  self._startIdPromise = startIdPromise = new Promise
  # Calling select right away queues the command before any commands that
  # a client might add before connect happens. If select is not queued first,
  # the subsequent commands could happen on the wrong db
  ignoreSubscribe = false
  do subscribeToStarts = (selected) ->
    return ignoreSubscribe = false if ignoreSubscribe
    if db isnt undefined && !selected
      return redisClient.select db, (err) ->
        throw err if err
        subscribeToStarts true
    redisInfo.subscribeToStarts subClient, redisClient, (starts) ->
      redisStarts = starts
      startIdPromise.clearValue() if startIdPromise.value
      startIdPromise.fulfill starts[0][0]

  # Ignore the first connect event
  ignoreSubscribe = true
  redisClient.on 'connect', subscribeToStarts
  redisClient.on 'end', ->
    redisStarts = null
    startIdPromise.clearValue()

  self._hasInvalidVer = (socket, ver, clientStartId) ->
    # Don't allow a client to connect unless there is a valid startId to
    # compare the model's against
    unless startIdPromise.value
      socket.disconnect()
      return true
    # TODO: Map the client's version number to the Stm's and update the client
    # with the new startId unless the client's version includes versions that
    # can't be mapped
    unless clientStartId && clientStartId == startIdPromise.value
      socket.emit 'fatalErr'
      return true
    return false
