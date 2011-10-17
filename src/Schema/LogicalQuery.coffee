{merge} = require '../util'
Promise = require '../Promise'

LogicalQuery = module.exports = (criteria) ->
  @_conditions = {}
  @find criteria if criteria
  return

LogicalQuery:: =
  where: (attr, val) ->
    @_conditions[attr] = val
    @

  find: (criteria, callback) ->
    @queryMethod = 'find'
    throw new Error 'Unimplmented'

  findOne: (criteria, callback) ->
    @queryMethod = 'findOne'
    if 'function' == typeof criteria
      callback = criteria
      criteria = null
    else if criteria.constructor == Object
      merge @_conditions, criteria

    return @ unless callback
    return @fire callback

  castConditions: ->
    conds = @_conditions
    fields = @schema._fields
    for k, v of conds
      field = fields[k]
      conds[k] = field.cast v if field.cast

  # Takes the state of the current query, and
  # fires off the query
  fire: (callback) ->
    @castConditions()
    queryMethod = @queryMethod
    Skema = @schema
    ns = Skema.namespace
    sources = Skema._sources
    if sources.length == 1
      source = sources[0]
      promise = new Promise
      promise.bothback callback
      source[queryMethod] ns, @_conditions, (err, castedJson) ->
        doc = new Skema castedJson
        return promise.resolve null, doc
      return promise

    promises = []
    for source in sources
      promise = new Promise
      source[queryMethod] ns, @_conditions, promise.resolve
      promises.push promise
    compositePromise = Promise.parallel promises...
    compositePromise.bothback callback
    return compositePromise

  # Binds this query to a Schema document
  bind: (schema) ->
    boundQuery = Object.create @
    boundQuery.schema = schema
    return boundQuery
