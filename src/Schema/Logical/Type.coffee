# # Fields/Attributes vs Types
# Types are indexed by string name via Type or Schema. They encapsulate code
# for basic casting and validation.
#
# Fields are when you associate a type with a field. It is a materialization of
# that type on a Schema attribute. This instance should be able to add its own
# validations in addition to inheriting the validations of the Type.
#
# # Array types and collection types
# These types are interesting in that they are similar to Haskell type variables
# or C templates.
#     Set<T>
#     [t]
#
#     # Maybe the following syntax
#     TypeClass.define 'Set', (type) ->
#

Field = require './Field'

Type = module.exports = (name, config) ->
  @_name       = name
  @_setups     = []
  @_validators = []

  for method, arg of config
    if method == 'cast'
      @cast = arg
      continue
    if Array.isArray arg
      @[method] arg...
    else
      @[method] arg
  return

Type:: =
  constructor: Type
  createField: -> new Field @
  assignAsTypeToSchemaField: (schema, fieldName) ->
    setups = @_setups
    setup.call schema, fieldName for setup in setups
    schema[fieldName] = @

  setup: (fn) ->
    @_setups.push fn
    return @

  caster: (caster) ->
    @cast = caster
    return @

  validator: (validator) ->
    @_validators.push validator
    return @

  extend: (@parentType) ->

  # TODO Add in async validations
  validate: (val) ->
    validators = @_validators
    errors = (err for fn in validators when (err = fn val) != true)
    if parentType = @parentType
      parentErrors = parentType.validate val
      unless true == parentErrors
        errors = errors.concat parentErrors
    return if errors.length then errors else true
