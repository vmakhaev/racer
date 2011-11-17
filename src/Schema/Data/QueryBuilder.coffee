Promise = require '../../Promise'

DataQueryBuilder = module.exports = (@_queryMethod) ->
  @_includeFields = {}
  @_logicalFields = {}
  @_fieldPromises = {}
  return

# TODO
DataQueryBuilder.condsRelTo = (dataField, LogicalSkema, conds) ->

DataQueryBuilder:: =
  # @param {LogicalField} logicalField
  # @param {Boolean} didNotFind indicates whether the success/failure of the previous DataQueryBuilder
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

  toQuery: ->
    fields        = @_includeFields
    queryMethod   = @_queryMethod

    fieldPaths = (path for _, {path} of fields)
    {ns} = fields[ fieldPaths[0] ]

    if fieldPaths.length
      DataSkema = @source.dataSchemasWithNs[ns]
      return DataSkema[queryMethod] @conds, select: fieldPaths

    # TODO Consider the following code. Remove or complete?
    throw new Error 'Unimplemented'
    prom = new Promise
    prom.fulfill null
    return prom
