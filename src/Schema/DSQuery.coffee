DSQuery = module.exports = (@conds, @queryMethod) ->
  @includeFields = {}
  @logicalFields = {}
  return

DSQuery.condsRelTo = (dataField, LogicalSkema, conds) ->

DSQuery:: =
  notifyAboutPrevQuery: (logicalField, didNotFind) ->
    logicalFields = @logicalFields
    logicalPath = logicalField.path
    dataFields = logicalFields[logicalPath]
    includeFields = @includeFields
    for dataField in dataFields
      delete includeFields[dataField.path] if didNotFind
    delete logicalFields[logicalPath]
    unless Object.keys(logicalFields).length
      @fire()

  add: (dataField) ->
    @source ||= dataField.source
    @includeFields[dataField.path] = dataField
    dataFields = @logicalFields[dataField.logicalField.path] ||= []
    dataFields.push dataField

  fire: ->
    fields = (field for _, field of @includeFields)
    if fields.length
      @source[@queryMethod] fields[0].rootNs, @conds, fields
