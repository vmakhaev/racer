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
