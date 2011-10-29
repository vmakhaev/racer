{merge} = require '../util'
Promise = require '../Promise'
Schema = require './index'
DSQueryDispatcher = require './DSQueryDispatcher'

LogicalQuery = module.exports = (criteria) ->
  @_conditions = {}
  @_opts = {}
  @find criteria if criteria
  return

LogicalQuery:: =
  where: (attr, val) ->
    @_conditions[attr] = val
    @

  find: (criteria, callback) ->
    @queryMethod = 'find'
    if 'function' == typeof criteria
      callback = criteria
      criteria = null
    else if criteria.constructor == Object
      merge @_conditions, criteria

    return @ unless callback
    return @fire callback

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
    fields = @schema.fields
    for k, v of conds
      field = fields[k]
      conds[k] = field.cast v if field.cast
    return conds

  # Takes the state of the current query, and fires off the query to all
  # data sources; then, collects and re-assembles the data into the
  # logical document and passes it to callback(err, doc)
  fire: (fireCallback) ->
    conds = @castConditions() # Logical schema casting of the query condition vals
    RootLogicalSkema = @schema
    # Conditions and fields to select will determine the async flow path
    # through (data source, namespace) nodes. Conditions may be for fields
    # spread between two data sources (e.g., name in one and age in another).
    # In this case, findOne and find should use different implementations
    # where findOne is more serial vs find which uses a parallel fanout
    # query followed by a merge+filter of the results, clustering attributes
    # by doc id.
    # Fields may be spread between two data sources
    if select = @_opts.select
      # In a select, fields could be deeply nested fields that belong to another Logical Schema
      # than the RootLogicalSkema firing this query.
      # TODO Add this kind of logic to @castConditions
      logicalFields = (RootLogicalSkema.lookupField path for path in select)
    else
      logicalFields = ([field, ''] for _, field of RootLogicalSkema.fields when ! (field.isRelationship))

    lFieldsPromises = []
    phaseForLField = {} # Maps logical field hashes -> curr index in logical field's readFlow
    qDispatcher = new DSQueryDispatcher @queryMethod
    for [logicalField, ownerPathRelToRoot] in logicalFields
      qDispatcher.registerLogicalField logicalField, conds
    firePromise = (new Promise).bothback fireCallback
    qDispatcher.fire (err, lFieldVals...) ->
      # TODO create a version of Promise.parallel that returns an Object as val in (err, val)
      # TODO This works for findOne, not find. How to implement for find efficiently?
      attrs = {}
      for [lFieldVal], i in lFieldVals
        [logicalField, ownerPathRelToRoot] = logicalFields[i]
        attrPath = ownerPathRelToRoot
        attrPath += '.' if attrPath
        attrPath += logicalField.path
        attrs[attrPath] = lFieldVal
      result = new RootLogicalSkema attrs, false
      firePromise.resolve null, result

    return firePromise



        # Aggregate data fields into groups that are then each retrieved in separate queries
        # Partition data fields by source, ns.
        # Dispatch first attempt db queries to each source in parallel
        # Handle results in a callback and dispatch additional queries if necessary
        # conds = DSQuery.condsRelTo dField, LogicalSkema, @_conditions

  # Binds this query to a CustomSchema
  bind: (schema) ->
    boundQuery = Object.create @
    boundQuery.schema = schema
    return boundQuery
