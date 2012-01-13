{merge} = require '../../util'
DataQueryBuilder = require './QueryBuilder'

FindOneBuilder = module.exports = (@DataSkema, @conds) ->
  @source = @DataSkema.source
  DataQueryBuilder.call @, 'findOne'
  return

FindOneBuilder:: = merge new DataQueryBuilder('findOne'),
  constructor: FindOneBuilder
  queryCallback: (err, json) ->
    fieldPromises = @_fieldPromises
    fields = @_includeFields
    for path, promise of fieldPromises
      promise.resolve err, json[path], fields[path]
    return
