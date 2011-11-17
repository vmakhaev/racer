Schema = require './Schema'

# Encapsulates the configuration of a field in a logical schema.
# A field is a declared attribute on a custom Schema that is associated with a type
# and configuration specific to the association of the type to this attribute -- e.g.,
# define custom validations at the LogicalField level that do not
# pollute the validation definitions set at the LogicalType or LogicalSchema
# level.

# @constructor LogicalField
# @param {Schema|Type} the type of this field
LogicalField = module.exports = (@type) ->
  @validators = []
  @dataFields = []
  @isRelationship = @type?.prototype instanceof Schema || (
    @type._name == 'Array' && (
      !@type.memberType || # If Array of to-be-defined Schema
      (@type.memberType.prototype instanceof Schema)
    )
  )
  return

LogicalField:: =
  schema: null
  path: null

  cast: (val, oplog) ->
    return val unless val?
    if @type.cast then @type.cast val, oplog else val

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
        dataFieldReadPhases.push [matches, parallelCb]
    else
      console.warn "Source lookup order not explicitly defined for this logical field `#{@path}` or its logical schema `#{@schema._name}`. Falling back to parallel fetching of `#{@path}` dataFields"
      dataFieldReadPhases.push [dataFields]
    return dataFieldReadPhases
