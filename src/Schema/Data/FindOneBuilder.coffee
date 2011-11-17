{merge} = require '../../util'
DataQueryBuilder = require './QueryBuilder'

FindOneBuilder = module.exports = (@conds) -> return

FindOneBuilder:: = merge new DataQueryBuilder('findOne'),
  queryCallback: (err, json) ->
    for path, promise of fieldPromises
      promise.resolve err, json[path], fields[path]
    return
