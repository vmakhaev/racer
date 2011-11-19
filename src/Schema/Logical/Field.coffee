LogicalSchema = require './Schema'

# Encapsulates the configuration of a field in a LogicalSchema.
# A LogicalField is a declared attribute on a custom LogicalSchema that's associated
# with a type & configuration specific to the association of the type to this attribute
# -- e.g., define custom validations at the LogicalField level that don't pollute
# the validation definitions set at the LogicalType or LogicalSchema level.

# @constructor LogicalField
# @param {LogicalSchema|Type} the type of this field
LogicalField = module.exports = (@type) ->
  @validators = []
  @dataFields = []
  @isRelationship = @type?.prototype instanceof LogicalSchema || (
    @type._name == 'Array' && (
      !@type.memberType || # If Array of to-be-defined LogicalSchema
      (@type.memberType.prototype instanceof LogicalSchema)
    )
  )
  return

LogicalField:: =
  schema: null
  path: null

  cast: (val, oplog, assignerDoc) ->
    return val unless val?
    if @type.cast then @type.cast val, oplog, assignerDoc else val

  # Defines a validator fn
  validator: (fn) ->
    @validators.push fn
    return @

  # Runs the defined validators against val
  # @return {Boolean|[Error]} returns true or an array of errors
  validate: (val) ->
    errors = []
    for fn in @validators
      result = fn val
      continue if true == result
      errors.push result

    result = @type.validate val
    errors = errors.concat result unless true == result

    return if errors.length then errors else true

  genDataFieldReadPhases: ->
    return dataFieldReadPhases if dataFieldReadPhases = @dataFieldReadPhases
    dataFieldReadPhases = @dataFieldReadPhases = []
    dataFields    = @dataFields

    if readFlow = @readFlow || @schema.readFlow
      for [sources, parallelCb] in readFlow
        matches = (f for f in dataFields when -1 != sources.indexOf f.source)
        matches.parallelCallback = parallelCb
        dataFieldReadPhases.push matches
    else
      console.warn "Source lookup order not explicitly defined for this logical field `#{@path}` or its logical schema `#{@schema._name}`. Falling back to parallel fetching of `#{@path}` dataFields"
      dataFieldReadPhases.push dataFields
    return dataFieldReadPhases

  expandPath: (pathToOwnerSchema) ->
    attrPath = pathToOwnerSchema
    attrPath += '.' if attrPath
    return attrPath += @path
