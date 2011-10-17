Promise = module.exports = (callback) ->
  @callbacks = []
  @errbacks = []
  @clearValueCallbacks = []
  @on callback if callback
  return

Promise:: =
  fulfill: (args...) ->
    if @value isnt undefined
      throw new Error 'Promise has already been fulfilled'
    if args.length == 1
      @value = args[0]
    else
      @value = args
    callback.apply scope, args for [callback, scope] in @callbacks
    @callbacks = []
    @

  error: (err) ->
    if @err
      throw new Error 'Promise has already erred'
    @err = val
    callback.call scope, err for [callback, scope] in @errbacks
    @errbacks = []
    @

  resolve: (err, val) ->
    return @error err if err
    return @fulfill val if val
    @

  on: (callback, scope) ->
    return callback.call scope, @value unless @value is undefined
    @callbacks.push [callback, scope]
    @

  errback: (callback, scope) ->
    return callback.call scope, @err if @err
    @errbacks.push [callback, scope]
    @

  bothback: (callback, scope) ->
    @errback callback, scope
    @on (val) ->
      callback.call @, null, val
    , scope

  onClearValue: (callback, scope) ->
    @clearValueCallbacks.push [callback, scope]
    @

  clearValue: ->
    delete @value
    cbs = @clearValueCallbacks
    callback.call scope for [callback, scope] in cbs
    @clearValueCallbacks = []
    @

Promise.parallel = (promises...) ->
  compositePromise = new Promise
  dependencies = promises.length
  for promise in promises
    promise.on -> --dependencies || compositePromise.fulfill(true)
    promise.onClearValue -> compositePromise.clearValue()
  return compositePromise
