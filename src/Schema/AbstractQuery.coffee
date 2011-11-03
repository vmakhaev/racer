{merge} = require '../util'
Promise = require '../Promise'
{deepCopy} = require '../util'

AbstractQuery = module.exports = (criteria) ->
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
    if 'function' == typeof criteria
      callback = criteria
      criteria = null
    else
      if 'function' == typeof opts
        callback = opts
        opts = null
      if criteria?.constructor == Object
        merge @_conditions, criteria
    
    @_applyOpts opts

    return @ unless callback
    return @fire callback

  # query.findOne({id: 1}, function (err, foundDoc) { /* */});
  # query.findOne({id: 1}, {select: ['*']}, function (err, foundDoc) { /* */});
  # query.findOne(function (err, foundDoc) { /* */});
  # query.findOne({id: 1});
  findOne: (criteria, opts, callback) ->
    @queryMethod = 'findOne'
    if 'function' == typeof criteria
      callback = criteria
      criteria = null
    else
      if 'function' == typeof opts
        callback = opts
        opts = null
      if criteria?.constructor == Object
        merge @_conditions, criteria

    @_applyOpts opts

    return @ unless callback
    return @fire callback

  _applyOpts: (opts) ->
    if opts
      for k, v of opts
        if Array.isArray v
          @[k] v...
        else
          @[k] v

  # @param {[String]} paths
  select: (paths...) ->
    @_selects.push paths...
    return @

  castConditions: ->
    return @schema.castObj @_conditions

  # Binds this query to a CustomSchema
  bind: (schema) ->
    boundQuery = Object.create @
    boundQuery.schema = schema
    return boundQuery
