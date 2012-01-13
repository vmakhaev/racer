{merge} = require '../../util'

DataField = module.exports = (@type, opts) ->
  @isVirtual = @type._name == 'Virtual'
  merge @, opts
  return

DataField:: =
  cast: (val) ->
    if @type.cast then @type.cast val else val
#   ns:
#   type:
#   source:
