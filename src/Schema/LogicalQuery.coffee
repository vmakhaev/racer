{merge} = require '../util'
AbstractQuery = require './AbstractQuery'
Promise = require '../Promise'
DSQueryDispatcher = require './DSQueryDispatcher'

LogicalQuery = module.exports = (criteria) ->
  AbstractQuery.call @, criteria
  return

LogicalQuery:: = merge new AbstractQuery(),
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
    selects = @_selects
    if selects.length
      # In a select, fields could be deeply nested fields that belong to another Logical Schema
      # than the RootLogicalSkema firing this query.
      # TODO Add this kind of logic to @castConditions
      logicalFields = (RootLogicalSkema.lookupField path for path in selects)
    else
      logicalFields = ([field, ''] for _, field of RootLogicalSkema.fields when ! (field.isRelationship))

    queryMethod = @queryMethod
    qDispatcher = new DSQueryDispatcher queryMethod
    for [logicalField, ownerPathRelToRoot] in logicalFields
      qDispatcher.registerLogicalField logicalField, conds
    firePromise = (new Promise).bothback fireCallback
    qDispatcher.fire (err, lFieldVals...) ->
      # TODO create a version of Promise.parallel that returns an Object as val in (err, val)
      switch queryMethod
        when 'findOne'
          attrs = {}
          for [lFieldVal], i in lFieldVals
            [logicalField, ownerPathRelToRoot] = logicalFields[i]
            attrPath = ownerPathRelToRoot
            attrPath += '.' if attrPath
            attrPath += logicalField.path
            attrs[attrPath] = lFieldVal
          result = new RootLogicalSkema attrs, false
        when 'find'
          memberByPkey = {}
          arrOfAttrs = []
          for [arrOfLFieldVals], i in lFieldVals
            continue if arrOfLFieldVals is undefined
            [logicalField, ownerPathRelToRoot] = logicalFields[i]
            attrPath = ownerPathRelToRoot
            attrPath += '.' if attrPath
            attrPath += logicalField.path
            for {pkeyVal, val} in arrOfLFieldVals
              unless member = memberByPkey[pkeyVal]
                member = memberByPkey[pkeyVal] = {}
                arrOfAttrs.push member
              member[attrPath] = val
          result = (new RootLogicalSkema attrs, false for attrs in arrOfAttrs)
            
      firePromise.resolve null, result

    return firePromise

LogicalQuery::constructor = LogicalQuery
