# Encapsulates the configuration of a field in a logical schema.
# A field is a declared attribute on a custom Schema that is associated with a type
# and configuration specific to the association of the type to this attribute -- e.g.,
# custom validations.

Field = module.exports = (@type) ->
  @validators = []
  return

Field:: =
  cast: (val) -> if @type.cast then @type.cast val else val
  validator: (fn) ->
    @validators.push fn
    return @

  validate: (val) ->
    errors = []
    for fn in @validators
      result = fn val
      continue if true == result
      errors.push result

    result = @type.validate val
    errors = errors.concat result unless true == result

    return if errors.length then errors else true
