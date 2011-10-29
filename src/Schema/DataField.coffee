{merge} = require '../util'


DataField = module.exports = (@type, opts) ->
  merge @, opts
  return

DataField:: =
  cast: (val) ->
    if @type.cast then @type.cast val else val
#   ns:
#   type:
#   source:
