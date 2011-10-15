Field = module.exports = (@type) ->
  @validators = []
  return

Field:: =
  cast: (val) -> @type.cast val
  validator: (fn) ->
    @validators.push fn
    return @

  validate: (val) ->
    errors = []
    for fn in @validators
      result = fn val
      continue if true == result
      errors.push result

    return if errors.length then errors else true
