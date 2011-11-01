Promise = module.exports = (callback) ->
  @callbacks = []
  @errbacks = []
  @clearValueCallbacks = []
  @callback callback if callback
  return

Promise:: =
  fulfill: (args...) ->
    if @fulfilled
      throw new Error 'Promise has already been fulfilled'
    @fulfilled = true
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
    @err = err
    callback.call scope, err for [callback, scope] in @errbacks
    @errbacks = []
    @

  resolve: (err, vals...) ->
    return @error err if err
    return @fulfill vals...
    @

  on: (callback, scope) ->
    return callback.call scope, @value if @fulfilled
    @callbacks.push [callback, scope]
    @

  errback: (callback, scope) ->
    return callback.call scope, @err if @err
    @errbacks.push [callback, scope]
    @

  bothback: (callback, scope) ->
    @errback callback, scope
    @callback (vals...) ->
      callback.call @, null, vals...
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

Promise::callback = Promise::on

Promise.parallel = (promises...) ->
  compositePromise = new Promise
  dependencies = promises.length
  parallelVals = []
  for promise, i in promises
    do (i) ->
      promise.callback (vals...) ->
        parallelVals[i] = vals
        --dependencies || compositePromise.fulfill parallelVals...
    promise.onClearValue -> compositePromise.clearValue()
  return compositePromise

Promise.transform = (transformFn) ->
  transPromise = new Promise
  origTransFulfill = transPromise.fulfill
  transPromise.fulfill = (val) ->
    origTransFulfill.call @, transformFn val
  return transPromise

Promise.pipe = (promiseA, promiseB) ->
  pipePromise = new Promise
  vals = []
  origPipeFulfill = pipePromise.fulfill
  # TODO Handle multiple arguments vals...
  pipePromise.fulfill = (val) ->
    promiseA.fulfill val
  promiseA.bothback (err, val) ->
    vals[0] = val unless err
    promiseB.resolve err, val
  promiseB.bothback (err, val) ->
    vals[1] = val unless err
    origPipeFulfill.resolve err, vals

  return pipePromise
