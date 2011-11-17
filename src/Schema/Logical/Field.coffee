Schema = require './Schema'

# Encapsulates the configuration of a field in a logical schema.
# A field is a declared attribute on a custom Schema that is associated with a type
# and configuration specific to the association of the type to this attribute -- e.g.,
# define custom validations at the Field level that do not
# pollute the validation definitions set at the Type or Schema level.

# @constructor Field
# @param {Schema|Type} the type of this field
Field = module.exports = (@type) ->
  @validators = []
  @dataFields = []
  @isRelationship = @type?.prototype instanceof Schema || (
    @type._name == 'Array' && (
      !@type.memberType || # If Array of to-be-defined Schema
      (@type.memberType.prototype instanceof Schema)
    )
  )
  return

Field:: =
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

  genDataFieldFlow: ->
    return dataFieldFlow if dataFieldFlow = @dataFieldFlow
    dataFieldFlow = []
    dataFields    = @dataFields

    if readFlow = @readFlow || @schema.readFlow
      for [sources, parallelCallback] in readFlow
        matchingDFields = (dField for dField in dataFields when -1 != sources.indexOf dField.source)
        dataFieldFlow.push [matchingDFields, parallelCallback]
    else
      console.warn "Source lookup order not explicitly defined for this logical field `#{@path}` or its logical schema `#{@schema._name}`. Falling back to parallel fetching of `#{@path}` dataFields"
      dataFieldFlow.push [dataFields]
    return @dataFieldFlow = dataFieldFlow
