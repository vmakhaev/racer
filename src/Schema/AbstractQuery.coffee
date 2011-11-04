{merge} = require '../util'
Promise = require '../Promise'
{deepCopy} = require '../util'

AbstractQuery = module.exports = (@schema, criteria) ->
  @_conditions = {}
  @_selects = []
  @find criteria if criteria
  return

AbstractQuery:: =
  clone: ->
    clone = Object.create @
    clone._conditions = deepCopy @_conditions
    clone._selects = @_selects.slice()
    return clone

  where: (attr, val) ->
    @_conditions[attr] = val
    @

  find: (criteria, opts, callback) ->
    @queryMethod = 'find'
    if typeof criteria is 'function'
      callback = criteria
      criteria = null
    else
      if typeof opts is 'function'
        callback = opts
        opts = null
      if criteria?.constructor == Object
        merge @_conditions, criteria
    @_applyOpts opts
    return @fire callback if callback
    return @

  # Possible ways to use findOne:
  # query.findOne({id: 1}, function (err, foundDoc) { /* */});
  # query.findOne({id: 1}, {select: ['*']}, function (err, foundDoc) { /* */});
  # query.findOne(function (err, foundDoc) { /* */});
  # query.findOne({id: 1});
  # 
  # @param {Object} criteria
  # @param {Object} opts
  # @param {Function} callback
  findOne: (criteria, opts, callback) ->
    @queryMethod = 'findOne'
    if typeof criteria is 'function'
      callback = criteria
      criteria = null
    else
      if typeof opts is 'function'
        callback = opts
        opts = null
      if criteria?.constructor == Object
        merge @_conditions, criteria
    @_applyOpts opts
    return @fire callback if callback
    return @

  _applyOpts: (opts) ->
    if opts then for k, v of opts
      if Array.isArray v
        @[k] v...
      else
        @[k] v

  # @param {[String]} paths
  select: (paths...) ->
    @_selects.push paths...
    return @

  _castConditions: -> return @schema.castObj @_conditions
