{merge} = require '../util'

MemorySource = module.exports = ->
  DataSource.apply @, arguments
  return

MemorySource:: = new DataSource()
merge MemorySource::,
  _queriesForOps: (oplog) ->
    # TODO

  addField: (field, config) ->
