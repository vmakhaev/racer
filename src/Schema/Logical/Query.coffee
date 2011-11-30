{merge} = require '../../util'
AbstractQuery = require '../AbstractQuery'
Promise = require '../../Promise'
QueryDispatcher = require '../QueryDispatcher'

LogicalQuery = module.exports = (schema, criteria) ->
  AbstractQuery.call @, schema, criteria
  return

LogicalQuery:: = merge new AbstractQuery(),
  # Takes the state of the current query, and fires off the query to all
  # data sources; then, collects and re-assembles the data into the
  # logical document and passes it to callback(err, doc)
  fire: (fireCallback) ->
    # Conditions and fields to select will determine the async flow path
    # through (data source, namespace) nodes.
    # TODO Conditions may be for fields
    # spread between two data sources (e.g., name in one and age in another).
    # In this case, findOne and find should use different implementations
    # where findOne is more serial vs find which uses a parallel fanout
    # query followed by a merge+filter of the results, clustering attributes
    # by doc id.
    # Fields may be spread between two data sources
    RootLogicalSkema = @schema
    selects = @_selects
    if selects.length
      # In a select, fields could be deeply nested fields that belong to another Logical Schema
      # than the RootLogicalSkema firing this query.
      # TODO Add this kind of logic to @_castConditions
      logicalFields = (RootLogicalSkema.lookupField path for path in selects)
    else
      remainder = ''
      logicalFields = ([field, remainder] for _, field of RootLogicalSkema.fields when ! (field.isRelationship))

    conds = @_castConditions() # Logical schema casting of the query condition vals
    queryMethod = @queryMethod
    qDispatcher = new QueryDispatcher queryMethod

    for [logicalField, schemaPath] in logicalFields
      qDispatcher.add logicalField, conds

    firePromise = new Promise bothback: fireCallback
    qDispatcher.fire (err, lFieldVals...) ->
      return firePromise.error err if err
      # TODO Maybe create a version of Promise.parallel that returns an Object as val in (err, val)
      switch queryMethod
        when 'findOne'
          attrs = {}
          for [lFieldVal], i in lFieldVals
            continue if lFieldVal is undefined
            [logicalField, schemaPath] = logicalFields[i]
            attrPath = logicalField.expandPath schemaPath
            attrs[attrPath] = lFieldVal
          result = new RootLogicalSkema attrs, false
        when 'find'
          memberByPkey = {}
          arrOfAttrs = []
          for [arrOfLFieldVals], i in lFieldVals
            continue if arrOfLFieldVals is undefined
            [logicalField, schemaPath] = logicalFields[i]
            attrPath = logicalField.expandPath schemaPath
            for {pkeyVal, val} in arrOfLFieldVals
              unless member = memberByPkey[pkeyVal]
                member = memberByPkey[pkeyVal] = {}
                arrOfAttrs.push member
              member[attrPath] = val
          result = (new RootLogicalSkema attrs, false for attrs in arrOfAttrs)

      firePromise.fulfill result

    return firePromise

LogicalQuery::constructor = LogicalQuery
