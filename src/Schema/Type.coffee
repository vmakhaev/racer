Type = module.exports = (name, config) ->
  @setups = []
  @casters = []
  @validators = []

  for method, arg of config
    if Array.isArray arg
      @[method] arg...
    else
      @[method] arg
  return

Type.extend = (name, config) ->
  SubType = (schema) ->
    return

  SubType:: = prototype = new @()
  prototype.constructor = SubType
  prototype.name = name

Type:: =
  # TODO Rename this
  assignAsTypeToSchemaField: (schema, fieldName) ->
    setups = @setups
    setup.call schema, fieldName for setup in setups
    schema[fieldName] = @

  setup: (fn) ->
    @setups.push fn
    return @

  caster: (caster) ->
    @casters.push caster
    return @

  validator: (validator) ->
    @validators.push validator
    return @

  extend: (parentType) ->

  cast: (val) ->
    (val = castFn.call @, val) for castFn in @casters
    return val
  
  validate: (val) ->

Field = (name, type, schema) ->
  @getters = []
  @setters = []
  return

Field:: =
  get: (getter) ->
    @getters.push getter
    return @

  set: (setter) ->
    @setters.push setter
    return @
