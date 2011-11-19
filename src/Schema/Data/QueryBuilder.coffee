Promise = require '../../Promise'

DataQueryBuilder = module.exports = (@_queryMethod) ->
  @_includeFields           = {}
  @_dataFieldsByLogicalPath = {}
  @_fieldPromises           = {}
  return

# TODO
DataQueryBuilder.condsRelTo = (dataField, LogicalSkema, conds) ->

DataQueryBuilder:: =
  notifyAboutPrevQuery: (logicalField, didFind) ->
    dFieldsByLPath  = @_dataFieldsByLogicalPath
    logicalPath     = logicalField.path
    dataFields      = dFieldsByLPath[logicalPath]
    includeFields = @_includeFields
    if didFind
      # If we found the result in the previous query, then no need to find it again
      delete includeFields[path] for {path} in dataFields
      delete dFieldsByLPath[logicalPath]
    @fire() unless Object.keys(dFieldsByLPath).length
    return

  # TODO? inversion of control, so that
  #       dataField.addToBuilder, so we can customize add on dataField's end
  add: (dataField, dataFieldProm) ->
    {logicalField, path} = dataField
    @_includeFields[path] = dataField
    @_fieldPromises[path] = dataFieldProm
    dataFields = @_dataFieldsByLogicalPath[logicalField.path] ||= []
    return dataFields.push dataField

  toQuery: ->
    fields        = @_includeFields
    queryMethod   = @_queryMethod

    fieldPaths = (path for _, {path} of fields)
    {ns} = fields[ fieldPaths[0] ]

    if fieldPaths.length
      DataSkema = @source.dataSchemasWithNs[ns]
      return DataSkema[queryMethod] @conds, select: fieldPaths

    console.warn 'The query has no fields to look up!'
    console.trace()
    return new Promise fulfill: undefined
