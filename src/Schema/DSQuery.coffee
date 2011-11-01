Promise = require '../Promise'

DSQuery = module.exports = (@conds, @queryMethod) ->
  @includeFields = {}
  @logicalFields = {}
  @fieldPromises = {}
  return

# TODO
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

  add: (dataField, dataFieldProm) ->
    @source ||= dataField.source
    fieldPath = dataField.path
    @includeFields[fieldPath] = dataField
    @fieldPromises[fieldPath] = dataFieldProm
    dataFields = @logicalFields[dataField.logicalField.path] ||= []
    dataFields.push dataField

  fire: ->
    fields = @includeFields
    anyFields = false
    for k of fields
      anyFields = true
      {ns} = fields[k]
      break
    fieldPromises = @fieldPromises
    if anyFields
      return @source.dataSchemasWithNs[ns][@queryMethod] @conds, {fields}, (err, json) ->
        for path, promise of fieldPromises
          promise.resolve err, json[path]

    # TODO Consider the following code. Remove or complete?
    throw new Error 'Unimplemented'
    prom = new Promise
    prom.fulfill null
    return prom
