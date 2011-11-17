Promise = require '../Promise'

# TODO Rename this - it's confusable with DataQuery
DSQuery = module.exports = (@conds, @_queryMethod) ->
  @_includeFields = {}
  @_logicalFields = {}
  @_fieldPromises = {}
  return

# TODO
DSQuery.condsRelTo = (dataField, LogicalSkema, conds) ->

DSQuery:: =
  # @param {LogicalField} logicalField
  # @param {Boolean} didNotFind indicates whether the success/failure of the previous DSQuery
  notifyAboutPrevQuery: (logicalField, didNotFind) ->
    logicalFields  = @_logicalFields
    logicalPath    = logicalField.path
    dataFields     = logicalFields[logicalPath]
    includeFields = @_includeFields
    for dataField in dataFields
      if didNotFind then delete includeFields[dataField.path]
    delete logicalFields[logicalPath]
    @fire() unless Object.keys(logicalFields).length
    return

  add: (dataField, dataFieldProm) ->
    @source ||= dataField.source
    fieldPath = dataField.path
    @_includeFields[fieldPath] = dataField
    @_fieldPromises[fieldPath] = dataFieldProm
    dataFields = @_logicalFields[dataField.logicalField.path] ||= []
    dataFields.push dataField

  fire: ->
    fields        = @_includeFields
    fieldPromises = @_fieldPromises

    # Extract field paths and the ns
    fieldPaths = []
    for k of fields
      {ns, path} = fields[k]
      fieldPaths.push path

    if fieldPaths.length
      queryMethod = @_queryMethod
      DataSkema   = @source.dataSchemasWithNs[ns]
      return DataSkema[queryMethod] @conds, {select: fieldPaths}, (err, json) ->
        switch queryMethod
          when 'find'
            for path, field of fields
              if field.type.isPkey
                pkeyPath = path
                break
            throw new Error 'Missing pkey path' unless pkeyPath
            resolveToByPath = {}
            for mem, i in json
              pkeyVal = mem[pkeyPath]
              for path, val of mem
                resolveToByPath[path] ||= []
                resolveToByPath[path][i] = {val, pkeyVal}
            for path, promise of fieldPromises
              promise.resolve err, resolveToByPath[path], fields[path]
          when 'findOne'
            for path, promise of fieldPromises
              promise.resolve err, json[path], fields[path]

    # TODO Consider the following code. Remove or complete?
    throw new Error 'Unimplemented'
    prom = new Promise
    prom.fulfill null
    return prom
